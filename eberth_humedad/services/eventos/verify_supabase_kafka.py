"""End-to-end check: real REST rows, Kafka ACK, consumption and local restart."""
import json
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from app import BOOTSTRAP, producer
from supabase_bridge import fetch_page, database, sweep

run = uuid.uuid4().hex
topic = 'aireiq-supabase-check-' + run[:12]
rows = fetch_page(0)[:20]
assert rows, 'Supabase no devolvio filas visibles'
fetch = lambda after: [r for r in rows if int(r['id']) > after]
admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
admin.create_topics([NewTopic(topic, 1, 1)])
admin.close()
pub = producer()
consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, auto_offset_reset='earliest',
                         enable_auto_commit=False, group_id='check-' + run)
try:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp)/'state.sqlite')
        db = database(path)
        first = sweep(db, fetch, pub, topic)
        expected = {r[0] for r in db.execute('SELECT event_id FROM sent')}
        db.close()
        db = database(path)
        second = sweep(db, fetch, pub, topic)
        db.close()
        assert first['published'] == len(rows) and second['published'] == 0
        seen = set()
        deadline = time.monotonic() + 60
        while seen != expected and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=1000).values():
                for msg in batch:
                    event = json.loads(msg.value)
                    assert event['event_id'] in expected
                    assert msg.key.decode() == event['dispositivo']
                    assert event['origen'] == 'supabase_poll' and event['replay'] is False
                    seen.add(event['event_id'])
        assert seen == expected, 'No se consumieron todos los eventos'
        report = dict(status='passed', run_id=run, topic=topic, real_supabase_read=True,
                      published=first['published'], consumed_unique=len(seen),
                      published_after_state_reopen=second['published'],
                      sample='primeras 20 filas visibles; no demuestra datos recientes del sensor',
                      completed_at=datetime.now(timezone.utc).isoformat())
        out = Path('/evidence/supabase')/run
        out.mkdir(parents=True)
        (out/'manifest.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps(report, indent=2), flush=True)
finally:
    consumer.close()
    pub.close()
