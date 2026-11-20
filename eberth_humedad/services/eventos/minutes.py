"""Small-volume live minute adapter. Explicit wall-clock deadline; no backfill forecast."""
import json
import math
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from app import BOOTSTRAP, producer, encode

SOURCE = 'aireiq-mediciones-supabase-v1'
TARGET = 'aireiq-minutos-vivos-v1'
GRACE = 10

def open_db(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript('''
    CREATE TABLE IF NOT EXISTS readings (
      id TEXT PRIMARY KEY, device TEXT, minute INTEGER, humidity REAL, temperature REAL, pressure REAL,
      received REAL, source_delay REAL);
    CREATE TABLE IF NOT EXISTS published (
      device TEXT, minute INTEGER, event_id TEXT, payload TEXT, partition_id INTEGER, offset_id INTEGER,
      PRIMARY KEY(device,minute));
    CREATE TABLE IF NOT EXISTS counters (reason TEXT PRIMARY KEY, n INTEGER);
    ''')
    return db

def count(db, reason):
    db.execute('INSERT INTO counters VALUES (?,1) ON CONFLICT(reason) DO UPDATE SET n=n+1', (reason,))

def accept(db, event, key, now):
    try:
        stamp = datetime.fromisoformat(event['event_time'])
        ingested = datetime.fromisoformat(event['ingested_at'])
        assert stamp.tzinfo and ingested.tzinfo
        ts = stamp.timestamp()
        minute = int(ts // 60) * 60
        device = event['dispositivo']
        assert isinstance(device, str) and device and key == device.encode()
        assert isinstance(event['event_id'], str) and event['event_id']
        assert event['schema_version'] == 1 and event['origen'] == 'supabase_poll' and event['replay'] is False
        vals = [event[k] for k in ('humedad_pct','temperatura_c','presion_hpa','gas_ohms')]
        assert all(type(v) in (float,int) and math.isfinite(v) for v in vals)
        h,t,p,g = vals
        assert 0 <= h <= 100 and -40 <= t <= 85 and 300 <= p <= 1100 and g > 0
    except (KeyError, ValueError, TypeError, AssertionError, OverflowError):
        reason = 'invalid'
    else:
        if ts > now + 5:
            reason = 'future_clock'
        elif now >= minute + 60 + GRACE:
            reason = 'past_deadline'
        elif db.execute('SELECT 1 FROM published WHERE device=? AND minute=?', (device,minute)).fetchone():
            reason = 'already_closed'
        else:
            cur = db.execute('INSERT OR IGNORE INTO readings VALUES (?,?,?,?,?,?,?,?)',
                             (event['event_id'],device,minute,h,t,p,now,ingested.timestamp()-ts))
            reason = 'accepted' if cur.rowcount else 'duplicate'
    count(db, reason)
    return reason

def close_ready(db, pub, now):
    rows = db.execute('''SELECT device,minute,count(*),avg(humidity),avg(temperature),avg(pressure),
                         min(received),max(received),max(source_delay)
                         FROM readings WHERE minute+60+?<=?
                         AND NOT EXISTS (SELECT 1 FROM published p WHERE p.device=readings.device AND p.minute=readings.minute)
                         GROUP BY device,minute ORDER BY minute,device''', (GRACE,now)).fetchall()
    for device,minute,n,h,t,p,first,last,maxdelay in rows:
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f'aireiq:live-minute-v1:{device}:{minute}'))
        event = dict(schema_version=1,event_id=event_id,dispositivo=device,
                     minuto=datetime.fromtimestamp(minute,timezone.utc).isoformat(),
                     humedad_pct=h,temperatura_c=t,presion_hpa=p,lecturas=n,
                     available_at=datetime.fromtimestamp(now,timezone.utc).isoformat(),
                     deadline_seconds=GRACE,closure_delay_seconds=now-minute-60,
                     first_received_at=first,last_received_at=last,max_source_delay_seconds=maxdelay,
                     origen='supabase_live_minute',replay=False,
                     incomplete_possible=True)
        metadata = pub.send(TARGET,key=device.encode(),value=encode(event)).get(timeout=30)
        with db:
            db.execute('INSERT INTO published VALUES (?,?,?,?,?,?)',
                       (device,minute,event_id,json.dumps(event),metadata.partition,metadata.offset))
        print(json.dumps({'minute_published':event}),flush=True)
    # Keep recent raw evidence; durable emitted aggregates remain available for auditing.
    with db:
        db.execute('DELETE FROM readings WHERE minute < ? AND EXISTS (SELECT 1 FROM published p WHERE p.device=readings.device AND p.minute=readings.minute)', (now-86400,))
    return len(rows)

def main():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        try: admin.create_topics([NewTopic(TARGET,3,1)])
        except TopicAlreadyExistsError: pass
    finally: admin.close()
    db = open_db('/state/minutes.sqlite')
    consumer = KafkaConsumer(SOURCE,bootstrap_servers=BOOTSTRAP,group_id='aireiq-live-minutes-v1',
                             enable_auto_commit=False,auto_offset_reset='earliest',max_poll_records=500)
    pub = producer()
    try:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            with db:
                for batch in batches.values():
                    for msg in batch:
                        try: event = json.loads(msg.value)
                        except (ValueError,UnicodeDecodeError): event = {}
                        if not isinstance(event,dict): event = {}
                        accept(db,event,msg.key,time.time())
            if batches: consumer.commit()
            close_ready(db,pub,time.time())
            status = dict(updated_at=datetime.now(timezone.utc).isoformat(),
                          counters=dict(db.execute('SELECT reason,n FROM counters')),
                          minutes_published=db.execute('SELECT count(*) FROM published').fetchone()[0])
            out = Path('/evidence/live_minutes'); out.mkdir(parents=True,exist_ok=True)
            temp = out/'status.tmp'; temp.write_text(json.dumps(status,indent=2),encoding='utf-8'); temp.replace(out/'status.json')
    finally:
        consumer.close(); pub.close(); db.close()

if __name__ == '__main__': main()
