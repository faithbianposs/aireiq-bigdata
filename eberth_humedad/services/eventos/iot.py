"""S7: reproducción explícita de CSV y verificación de ingesta Kafka."""
import argparse
import csv
import hashlib
import json
import math
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from app import BOOTSTRAP, producer, encode, now

TOPIC = 'aireiq-mediciones-v1'
FIELDS = ['temperatura_c', 'humedad_pct', 'presion_hpa', 'gas_ohms']

def topic_init():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()

def make_event(row, run_id):
    stamp_text = row['created_at'].strip().replace('T', ' ').replace('Z', '+00:00')
    stamp_text = re.sub(r'([+-]\d{2})$', r'\1:00', stamp_text)
    timestamp = None
    for fmt in ['%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z']:
        try:
            timestamp = datetime.strptime(stamp_text, fmt)
            break
        except ValueError:
            pass
    if timestamp is None:
        raise ValueError('Fecha de origen no convertible con zona horaria')
    if timestamp.tzinfo is None:
        raise ValueError('La fecha de origen necesita zona horaria')
    stamp = timestamp.astimezone(timezone.utc).isoformat()
    device = row['dispositivo'].strip()
    if not device or not row['id'].strip():
        raise ValueError('Falta dispositivo o ID')
    identity = f"mediciones_aire:{device}:{row['id']}:{stamp}"
    values = {field: float(row[field]) for field in FIELDS}
    if not all(math.isfinite(v) for v in values.values()):
        raise ValueError('Numero no finito en el CSV')
    return dict(schema_version=1, event_id=str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
                source_id=row['id'], dispositivo=device, event_time=stamp, ingested_at=now(),
                origen='reproduccion_csv', replay=True, replay_run_id=run_id, **values)

def validate(e, key):
    reasons = []
    if e.get('schema_version') != 1:
        reasons.append('version')
    for field in ['event_id', 'dispositivo', 'event_time', 'ingested_at', 'source_id', 'replay_run_id']:
        if not isinstance(e.get(field), str) or not e[field].strip():
            reasons.append('campo:' + field)
    try:
        uuid.UUID(e['event_id'])
        for field in ['event_time', 'ingested_at']:
            if datetime.fromisoformat(e[field]).tzinfo is None:
                reasons.append('zona:' + field)
    except (ValueError, KeyError, TypeError):
        reasons.append('identidad_o_fecha')
    if key != str(e.get('dispositivo', '')).encode():
        reasons.append('key')
    if e.get('replay') is not True or e.get('origen') != 'reproduccion_csv':
        reasons.append('origen')
    for field in FIELDS:
        if type(e.get(field)) not in [int, float] or not math.isfinite(e[field]):
            reasons.append('numero:' + field)
    if reasons:
        return reasons
    for field, lo, hi in [('humedad_pct',0,100), ('temperatura_c',-40,85), ('presion_hpa',300,1100)]:
        if not lo <= e[field] <= hi:
            reasons.append('rango:' + field)
    if e['gas_ohms'] <= 0:
        reasons.append('fallo_sensor_gas')
    return reasons

def test_contract():
    row = dict(id='1', dispositivo='ESP32_TEST', created_at='2026-11-19T12:00:00-05:00',
               temperatura_c='20', humedad_pct='40', presion_hpa='645', gas_ohms='1000')
    good = make_event(row, 'test')
    assert not validate(good, b'ESP32_TEST')
    assert make_event(row, 'otra-reproduccion')['event_id'] == good['event_id']
    assert validate({**good, 'gas_ohms':0}, b'ESP32_TEST') == ['fallo_sensor_gas']
    assert validate({**good, 'humedad_pct':101}, b'ESP32_TEST')
    assert validate({**good, 'humedad_pct':float('nan')}, b'ESP32_TEST')
    assert validate(good, b'otra-key')
    assert validate({**good, 'event_time':'fecha-invalida'}, b'ESP32_TEST')
    exact_csv = make_event({**row, 'created_at':'2026-10-21 23:54:51.627012-05'}, 'test')
    assert exact_csv['event_time'] == '2026-10-22T04:54:51.627012+00:00'
    short_fraction = make_event({**row, 'created_at':'2026-10-21 23:54:51.62701-05'}, 'test')
    assert short_fraction['event_time'] == '2026-10-22T04:54:51.627010+00:00'
    return 9

def run(path, pause, limit):
    checks = test_contract()
    topic_init()
    run_id = str(uuid.uuid4())
    out = Path('/evidence/s07') / run_id
    out.mkdir(parents=True)
    # Un grupo por ensayo; auto_offset_reset=latest no pierde los eventos porque
    # se asignan particiones y se posiciona al final ANTES de publicar.
    c = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP, group_id='aireiq-s07-' + run_id,
                      enable_auto_commit=False, auto_offset_reset='earliest', max_poll_interval_ms=900000)
    deadline = time.monotonic() + 60
    while not c.assignment() and time.monotonic() < deadline:
        c.poll(timeout_ms=500)
    assert c.assignment(), 'No se asignaron particiones'
    c.seek_to_end()
    start = {str(tp.partition):c.position(tp) for tp in c.assignment()}
    p = producer()
    expected = {}
    source_rejected = 0
    try:
        with path.open(encoding='utf-8-sig', newline='') as f, (out/'errores_fuente.jsonl').open('w', encoding='utf-8') as errors:
            for i, row in enumerate(csv.DictReader(f)):
                if limit and i >= limit:
                    break
                try:
                    e = make_event(row, run_id)
                except (ValueError, KeyError, TypeError) as exc:
                    source_rejected += 1
                    errors.write(json.dumps({'line':i+2, 'reason':str(exc)})+'\n')
                    continue
                m = p.send(TOPIC, key=e['dispositivo'].encode(), value=encode(e)).get(timeout=30)
                assert e['event_id'] not in expected, 'ID repetido en fuente; resolver antes de demostrar'
                expected[e['event_id']] = (m.partition, m.offset)
                if (i+1) % 1000 == 0:
                    print(f'Publicadas {i+1} lecturas historicas', flush=True)
                if pause:
                    time.sleep(pause)
        p.flush()
        print('Publicacion terminada. Verificando consumo...', flush=True)
        accepted = rejected = duplicates = 0
        seen = set()
        by_reason = {}
        partitions = set()
        deadline = time.monotonic() + 120
        with (out/'rechazos_sensor.jsonl').open('w',encoding='utf-8') as quarantine:
            while len(seen) < len(expected) and time.monotonic() < deadline:
                for batch in c.poll(timeout_ms=1000).values():
                    for msg in batch:
                        e = json.loads(msg.value)
                        if e.get('replay_run_id') != run_id:
                            continue
                        assert e['event_id'] in expected
                        assert expected[e['event_id']] == (msg.partition,msg.offset)
                        if e['event_id'] in seen:
                            duplicates += 1
                            continue
                        seen.add(e['event_id'])
                        partitions.add(msg.partition)
                        reasons = validate(e, msg.key)
                        if reasons:
                            rejected += 1
                            for reason in reasons:
                                by_reason[reason] = by_reason.get(reason,0)+1
                            quarantine.write(json.dumps({'event':e,'motivos':reasons,'partition':msg.partition,'offset':msg.offset})+'\n')
                        else:
                            accepted += 1
            assert len(expected) > 0 and len(seen) == len(expected), 'Faltan mensajes consumidos'
            assert accepted + rejected == len(expected)
            c.commit()
        end = {str(tp.partition):c.position(tp) for tp in c.assignment()}
        report = dict(run_id=run_id, completed_at=now(), topic=TOPIC, source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                      origen='reproduccion_csv', sensor_en_vivo=False, publicados=len(expected), consumidos=len(seen),
                      validos=accepted, rechazados_sensor=rejected, rechazados_fuente=source_rejected,
                      duplicados=duplicates, motivos=by_reason, particiones_usadas=sorted(partitions),
                      offsets_inicio=start, offsets_fin=end, contract_checks=checks, pausa_segundos=pause, status='passed')
        (out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report,indent=2), flush=True)
    finally:
        p.close()
        c.close()

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv',default='/data/mediciones_aire_rows.csv')
    ap.add_argument('--pausa',type=float,default=0.25)
    ap.add_argument('--limite',type=int,default=0)
    args = ap.parse_args()
    assert args.pausa >= 0 and args.limite >= 0
    run(Path(args.csv), args.pausa, args.limite)
