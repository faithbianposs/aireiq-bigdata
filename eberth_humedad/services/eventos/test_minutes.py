import json
import tempfile
import unittest
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from minutes import open_db,accept,close_ready

BASE=1800000000 # exact UTC minute
def event(i,seconds,h=40):
    return dict(schema_version=1,event_id=str(i),dispositivo='D',event_time=datetime.fromtimestamp(BASE+seconds,timezone.utc).isoformat(),
                ingested_at=datetime.fromtimestamp(BASE+seconds+2,timezone.utc).isoformat(),origen='supabase_poll',replay=False,
                humedad_pct=h,temperatura_c=20,presion_hpa=645,gas_ohms=1000)
class Pub:
    def __init__(self,fail=False): self.events=[]; self.fail=fail
    def send(self,topic,key,value): self.events.append(json.loads(value)); return self
    def get(self,timeout):
        if self.fail: raise TimeoutError()
        return SimpleNamespace(partition=0,offset=len(self.events)-1)
class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.path=str(Path(self.tmp.name)/'state.db'); self.db=open_db(self.path)
    def tearDown(self): self.db.close(); self.tmp.cleanup()
    def test_deadline_average_duplicate_restart(self):
        with self.db:
            self.assertEqual(accept(self.db,event(1,5),b'D',BASE+7),'accepted')
            self.assertEqual(accept(self.db,event(1,5),b'D',BASE+8),'duplicate')
            self.assertEqual(accept(self.db,event(2,50,60),b'D',BASE+59),'accepted')
        p=Pub(); self.assertEqual(close_ready(self.db,p,BASE+69),0)
        self.assertEqual(close_ready(self.db,p,BASE+70),1)
        self.assertEqual(p.events[0]['humedad_pct'],50)
        self.assertEqual(p.events[0]['lecturas'],2)
        self.assertEqual(accept(self.db,event(3,55),b'D',BASE+70),'past_deadline')
        self.db.commit(); self.db.close(); self.db=open_db(self.path)
        self.assertEqual(close_ready(self.db,p,BASE+80),0)
    def test_ack_failure_preserves_pending(self):
        with self.db: accept(self.db,event(1,5),b'D',BASE+7)
        failed=Pub(True)
        with self.assertRaises(TimeoutError): close_ready(self.db,failed,BASE+70)
        p=Pub(); self.assertEqual(close_ready(self.db,p,BASE+71),1)
        self.assertEqual(p.events[0]['event_id'],failed.events[0]['event_id'])
    def test_quality_and_future(self):
        self.assertEqual(accept(self.db,event(1,5,101),b'D',BASE+7),'invalid')
        self.assertEqual(accept(self.db,event(2,50),b'D',BASE+7),'future_clock')
        self.assertEqual(accept(self.db,event(3,5),b'other',BASE+7),'invalid')
if __name__=='__main__': unittest.main(verbosity=2)
