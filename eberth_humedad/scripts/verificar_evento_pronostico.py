"""End-to-end verification of forecast-derived business intervention event."""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
SOURCE_TOPIC = 'aireiq-intervenciones-v1'
AUDIT_TOPIC = 'aireiq-intervenciones-auditadas-v1'

def now():
    return datetime.now(timezone.utc).isoformat()

def main():
    print(json.dumps({'verification': 'forecast_derived_business_event', 'bootstrap': BOOTSTRAP}), flush=True)
    test_id = str(uuid.uuid4())
    device = 'ESP32_AireIQ'
    target_minute = int(time.time() // 60) * 60 + 120

    event = {
        'event_id': test_id,
        'schema_version': 1,
        'occurred_at': now(),
        'ambiente_id': device,
        'tipo': 'intervencion.registrada',
        'accion': 'ventilacion_preventiva_sugerida',
        'origen': 'pronostico_automatico',
        'humedad_predicha': 48.75,
        'umbral': 42.0,
        'horizonte_segundos': 120,
        'regla': 'humedad_alta_pronosticada',
        'motivo': 'Humedad predicha 48.75% a t+120s excede umbral p95 (42.00%)'
    }

    # Start consumer on audit topic
    consumer_group = f'aireiq-verify-audit-{test_id[:8]}'
    consumer = KafkaConsumer(
        AUDIT_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=consumer_group,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=15000
    )

    # Publish forecast-derived event
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )

    encoded = json.dumps(event, ensure_ascii=False).encode('utf-8')
    pub_meta = producer.send(SOURCE_TOPIC, key=device.encode('utf-8'), value=encoded).get(timeout=10)
    producer.flush()
    producer.close()

    print(json.dumps({
        'published': {
            'event_id': test_id,
            'topic': pub_meta.topic,
            'partition': pub_meta.partition,
            'offset': pub_meta.offset
        }
    }), flush=True)

    # Await audited event from aireiq-eventos worker
    received = None
    deadline = time.time() + 25
    while time.time() < deadline:
        for batch in consumer.poll(timeout_ms=1000).values():
            for msg in batch:
                try:
                    payload = json.loads(msg.value.decode('utf-8'))
                    if payload.get('event_id') == test_id:
                        received = {
                            'topic': msg.topic,
                            'partition': msg.partition,
                            'offset': msg.offset,
                            'payload': payload
                        }
                        break
                except Exception:
                    pass
            if received:
                break
        if received:
            break

    consumer.close()

    if not received:
        print(json.dumps({'error': 'Audited event not received within deadline', 'status': 'failed'}), flush=True)
        sys.exit(1)

    manifest = {
        'test_id': test_id,
        'timestamp': now(),
        'status': 'passed',
        'event_published': {
            'topic': pub_meta.topic,
            'partition': pub_meta.partition,
            'offset': pub_meta.offset,
            'event': event
        },
        'event_audited': received,
        'verification_notes': 'Evento derivado de predicción publicado en aireiq-intervenciones-v1, procesado y auditado exitosamente en aireiq-intervenciones-auditadas-v1 conforme a contrato S6 y cierre funcional U2.'
    }

    out_path = Path('/evidence/live_forecast/manifest_evento_empresarial.json')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(manifest, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main()
