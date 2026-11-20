import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from supabase_bridge import database, sweep

def row(i):
    return dict(id=i, created_at='2026-11-19T12:00:00-05:00', dispositivo='ESP32_TEST',
                temperatura_c=20, humedad_pct=40, presion_hpa=645, gas_ohms=1000)

class Producer:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail
    def send(self, topic, key, value):
        self.events.append(json.loads(value))
        return self
    def get(self, timeout):
        if self.fail:
            raise TimeoutError('simulated missing acknowledgement')
        return SimpleNamespace(partition=0, offset=len(self.events)-1)

class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name)/'state.sqlite')
        self.db = database(self.path)
    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()
    def test_pagination_restart_and_delayed_commit(self):
        rows = [row(1), row(3)]
        fetch = lambda after: [r for r in rows if r['id'] > after][:1]
        pub = Producer()
        self.assertEqual(sweep(self.db, fetch, pub)['published'], 2)
        self.db.close()
        self.db = database(self.path)
        rows.insert(1, row(2))  # ID committed late, below previous maximum.
        self.assertEqual(sweep(self.db, fetch, pub)['published'], 1)
        self.assertEqual(sweep(self.db, fetch, pub)['published'], 0)
        self.assertTrue(all(e['replay'] is False for e in pub.events))
        self.assertEqual(len({e['event_id'] for e in pub.events}), 3)
    def test_failed_ack_is_retried_with_same_id(self):
        fetch = lambda after: [row(1)] if after == 0 else []
        failed = Producer(True)
        with self.assertRaises(TimeoutError):
            sweep(self.db, fetch, failed)
        self.assertEqual(self.db.execute('SELECT count(*) FROM sent').fetchone()[0], 0)
        good = Producer()
        sweep(self.db, fetch, good)
        self.assertEqual(failed.events[0]['event_id'], good.events[0]['event_id'])
    def test_resumes_from_window_not_from_zero(self):
        rows = [row(i) for i in range(1, 2001)]
        asked = []
        def fetch(after):
            asked.append(after)
            return [r for r in rows if r['id'] > after][:500]
        self.assertEqual(sweep(self.db, fetch, Producer())['published'], 2000)
        asked.clear()
        rows.append(row(2001))
        self.assertEqual(sweep(self.db, fetch, Producer(), lookback=500)['published'], 1)
        self.assertEqual(asked[0], 1500)  # starts at max sent (2000) - lookback, not at 0
    def test_invalid_row_does_not_advance(self):
        with self.assertRaises(ValueError):
            sweep(self.db, lambda after: [{**row(1), 'created_at': 'invalid'}], Producer())
        self.assertEqual(self.db.execute('SELECT count(*) FROM sent').fetchone()[0], 0)

if __name__ == '__main__':
    unittest.main(verbosity=2)
