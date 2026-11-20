"""S6: registro y auditoría de eventos de negocio. No controla hardware."""
import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'aireiq-intervenciones-v1'
AUDIT = 'aireiq-intervenciones-auditadas-v1'
DLQ = 'aireiq-intervenciones-invalidas-v1'
GROUP = 'aireiq-auditoria-v1'

def now():
    return datetime.now(timezone.utc).isoformat()

def encode(value):
    return json.dumps(value, ensure_ascii=False).encode('utf-8')

def producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', retries=5,
                         max_in_flight_requests_per_connection=1)

def init():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        for name in (TOPIC, AUDIT, DLQ):
            try:
                admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()

def validate(e):
    if not isinstance(e, dict):
        raise ValueError('El evento debe ser un objeto')
    for field in ['event_id', 'occurred_at', 'ambiente_id', 'tipo', 'accion', 'origen']:
        if not isinstance(e.get(field), str) or not e[field].strip():
            raise ValueError('Campo ausente o invalido: ' + field)
    uuid.UUID(e['event_id'])
    stamp = datetime.fromisoformat(e['occurred_at'].replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('occurred_at requiere zona horaria')
    if type(e.get('schema_version')) is not int or e['schema_version'] != 1:
        raise ValueError('schema_version debe ser 1')
    if e['tipo'] != 'intervencion.registrada':
        raise ValueError('tipo no soportado')
    if e['accion'] not in ['ventilacion_manual_registrada', 'mantenimiento_registrado', 'ventilacion_preventiva_sugerida']:
        raise ValueError('accion no soportada')
    if e['origen'] not in ['simulacion', 'operador', 'pronostico_automatico']:
        raise ValueError('origen no soportado')
    return e

def event(ambiente, accion, origen):
    return validate(dict(event_id=str(uuid.uuid4()), schema_version=1, occurred_at=now(),
                         ambiente_id=ambiente, tipo='intervencion.registrada', accion=accion, origen=origen))

def publish(e):
    validate(e)
    p = producer()
    try:
        m = p.send(TOPIC, key=e['ambiente_id'].encode(), value=encode(e)).get(timeout=30)
        return dict(event_id=e['event_id'], topic=m.topic, partition=m.partition, offset=m.offset)
    finally:
        p.close()

def worker():
    init()
    c = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP, group_id=GROUP,
                      enable_auto_commit=False, auto_offset_reset='earliest')
    p = producer()
    print(json.dumps({'status': 'consumiendo', 'grupo': GROUP}), flush=True)
    try:
        for message in c:
            try:
                e = validate(json.loads(message.value))
                if message.key != e['ambiente_id'].encode():
                    raise ValueError('key distinta de ambiente_id')
            except (ValueError, TypeError, UnicodeDecodeError) as exc:
                result = dict(tipo='evento.rechazado', motivo=str(exc), received_at=now(),
                              source_topic=message.topic, source_partition=message.partition,
                              source_offset=message.offset, raw=message.value.decode('utf-8', errors='replace'))
                target = DLQ
            else:
                result = dict(tipo='intervencion.auditada', schema_version=1, event_id=e['event_id'],
                              ambiente_id=e['ambiente_id'], origen=e['origen'], audited_at=now(),
                              source_partition=message.partition, source_offset=message.offset)
                target = AUDIT
            # Confirmar salida antes de confirmar offset. Una caída intermedia puede repetir la salida.
            m = p.send(target, key=message.key, value=encode(result)).get(timeout=30)
            c.commit()
            print(json.dumps({'target':target, 'partition':m.partition, 'offset':m.offset,
                              'source_offset':message.offset, 'event_id':result.get('event_id')}), flush=True)
    finally:
        c.close()
        p.close()

def smoke():
    init()
    run = str(uuid.uuid4())
    consumer = KafkaConsumer(AUDIT, DLQ, bootstrap_servers=BOOTSTRAP,
                             group_id='aireiq-prueba-' + run, auto_offset_reset='earliest',
                             enable_auto_commit=False)
    valid = [event('ambiente-demo', 'ventilacion_manual_registrada', 'simulacion') for _ in range(3)]
    p = producer()
    # Inválido primero, luego válidos: evidencia de que el consumidor continúa.
    bad = {'schema_version': 1, 'test_run': run}
    bad_meta = p.send(TOPIC, key=b'ambiente-demo', value=encode(bad)).get(timeout=30)
    sent = []
    for e in valid:
        m = p.send(TOPIC, key=b'ambiente-demo', value=encode(e)).get(timeout=30)
        sent.append(dict(event_id=e['event_id'], topic=m.topic, partition=m.partition, offset=m.offset))
    p.close()
    expected = {e['event_id'] for e in valid}
    received, rejected = {}, None
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline and (set(received) != expected or rejected is None):
            for batch in consumer.poll(timeout_ms=1000).values():
                for msg in batch:
                    result = json.loads(msg.value)
                    if msg.topic == AUDIT and result.get('event_id') in expected:
                        received[result['event_id']] = dict(topic=msg.topic, partition=msg.partition, offset=msg.offset, event=result)
                    if msg.topic == DLQ and result.get('source_partition') == bad_meta.partition and result.get('source_offset') == bad_meta.offset:
                        rejected = result
        assert set(received) == expected, 'No llegaron todas las auditorias'
        assert rejected is not None, 'No se recibio rechazo'
        assert len({m['partition'] for m in sent}) == 1, 'Misma key en diferentes particiones'
        report = dict(run_id=run, completed_at=now(), origen='simulacion', published=sent,
                      consumed=list(received.values()), invalid_handled=rejected,
                      same_key_same_partition=True, status='passed', semantics='at-least-once')
        path = Path('/evidence/s06') / run
        path.mkdir(parents=True)
        (path / 'manifest.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        consumer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['init', 'worker', 'publish', 'smoke'])
    parser.add_argument('--ambiente', default='ambiente-demo')
    parser.add_argument('--accion', default='ventilacion_manual_registrada')
    parser.add_argument('--origen', choices=['simulacion', 'operador'], default='simulacion')
    args = parser.parse_args()
    if args.command == 'publish':
        print(json.dumps(publish(event(args.ambiente, args.accion, args.origen))))
    else:
        {'init':init, 'worker':worker, 'smoke':smoke}[args.command]()
