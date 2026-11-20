"""Read-only polling bridge. Each sweep revisits IDs to catch delayed commits."""
import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from app import BOOTSTRAP, producer, encode
from iot import make_event

TOPIC = 'aireiq-mediciones-supabase-v1'
SELECT = 'id,created_at,dispositivo,temperatura_c,humedad_pct,presion_hpa,gas_ohms'

def fetch_page(after):
    url = os.environ.get('SUPABASE_URL', '').rstrip('/')
    key = os.environ.get('SUPABASE_API_KEY', '')
    parts = urlsplit(url)
    if parts.scheme != 'https' or not parts.netloc or parts.query or parts.fragment or parts.path:
        raise ValueError('Configurar SUPABASE_URL con la URL HTTPS base del proyecto')
    if not key:
        raise ValueError('Falta SUPABASE_API_KEY en el entorno')
    headers = {'apikey': key, 'Accept-Profile': 'public'}
    if key.startswith('eyJ'):  # Legacy JWT keys; new keys are not bearer JWTs.
        headers['Authorization'] = 'Bearer ' + key
    query = urlencode({'select': SELECT, 'id': f'gt.{after}', 'order': 'id.asc', 'limit': 500})
    request = Request(url + '/rest/v1/mediciones_aire?' + query, headers=headers)
    try:
        with urlopen(request, timeout=30) as response:
            rows = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f'Supabase HTTP {exc.code}; revisar acceso y politica SELECT') from None
    if not isinstance(rows, list):
        raise ValueError('Respuesta Supabase no es una lista')
    return rows

def database(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE IF NOT EXISTS sent (event_id TEXT PRIMARY KEY, source_id TEXT, partition_id INTEGER, offset_id INTEGER)')
    return db

def sweep(db, fetch, publish, topic=TOPIC):
    after = 0
    sent = seen = 0
    while True:
        rows = fetch(after)
        if not rows:
            return {'published': sent, 'already_sent': seen}
        for row in rows:
            ident = int(row['id'])
            if ident <= after:
                raise ValueError('La fuente debe devolver IDs positivos estrictamente ascendentes')
            # Stop on unconvertible rows: never silently skip source data.
            event = make_event({**row, 'id': str(ident)}, 'supabase-live-v1')
            event.update(origen='supabase_poll', replay=False)
            if db.execute('SELECT 1 FROM sent WHERE event_id=?', (event['event_id'],)).fetchone():
                seen += 1
            else:
                metadata = publish.send(topic, key=event['dispositivo'].encode(), value=encode(event)).get(timeout=30)
                # Commit only AFTER Kafka acknowledgement. A crash here may replay the same ID.
                with db:
                    db.execute('INSERT INTO sent VALUES (?,?,?,?)',
                               (event['event_id'], event['source_id'], metadata.partition, metadata.offset))
                sent += 1
            after = ident

def init_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        try:
            admin.create_topics([NewTopic(TOPIC, 3, 1)])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    # Check access before creating Kafka state. Empty responses do not prove RLS access.
    sample = fetch_page(0)
    print(json.dumps({'api_read': True, 'visible_rows_first_page': len(sample),
                      'note': 'Una respuesta vacia no demuestra acceso a filas bajo RLS'}), flush=True)
    if args.check:
        return
    init_topic()
    db = database('/state/supabase.sqlite')
    pub = producer()
    try:
        while True:
            print(json.dumps(sweep(db, fetch_page, pub)), flush=True)
            if args.once:
                break
            time.sleep(max(5, int(os.environ.get('SUPABASE_POLL_SECONDS', '15'))))
    finally:
        pub.close()
        db.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # No response bodies, request headers or credentials in logs.
        print('PUENTE DETENIDO:', type(exc).__name__, flush=True)
        raise SystemExit(1)
