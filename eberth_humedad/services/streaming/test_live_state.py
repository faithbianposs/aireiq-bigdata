import tempfile
import unittest
from pathlib import Path
from live_state import connect,record,snapshot,evaluate_intervention

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=str(Path(self.tmp.name)/'state.sqlite');self.db=connect(self.path)
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def test_exact_target_and_no_future_label(self):
        record(self.db,'D',0,40,70,3,41,75,'m')
        self.assertEqual(snapshot(self.db)['scored'],0)
        record(self.db,'D',60,90,130,3,90,135,'m')
        self.assertEqual(snapshot(self.db)['scored'],0)
        record(self.db,'D',120,43,190,3,43,195,'m')
        self.assertEqual(snapshot(self.db)['scored'],1)
        self.assertEqual(snapshot(self.db)['rmse'],2)
        self.assertEqual(snapshot(self.db)['baseline_rmse'],3)
    def test_late_boundary_and_duplicate_restart(self):
        record(self.db,'D',0,40,70,1,41,120,'m')
        record(self.db,'D',120,43,190,1,43,195,'m')
        self.db.close();self.db=connect(self.path)
        record(self.db,'D',0,40,70,1,99,200,'m')
        values=snapshot(self.db)
        self.assertEqual(values['observations'],2)
        self.assertEqual(values['late_predictions'],1)
        self.assertEqual(values['scored'],0)
    def test_gap_and_other_device_not_scored(self):
        record(self.db,'D',0,40,70,1,41,75,'m')
        record(self.db,'OTHER',120,43,190,1,43,195,'m')
        record(self.db,'D',180,43,250,1,43,255,'m')
        self.assertEqual(snapshot(self.db)['scored'],0)
    def test_intervention_evaluation_and_cooldown(self):
        # Normal prediction within [30.0, 42.0] -> no intervention
        event_normal = evaluate_intervention(self.db, 'D', 0, 35.0, 75, upper_threshold=42.0, lower_threshold=30.0, cooldown_seconds=300)
        self.assertIsNone(event_normal)
        self.assertEqual(snapshot(self.db)['interventions_emitted'], 0)

        # High humidity prediction > 42.0 -> triggers intervention
        event_high = evaluate_intervention(self.db, 'D', 60, 45.5, 135, upper_threshold=42.0, lower_threshold=30.0, cooldown_seconds=300)
        self.assertIsNotNone(event_high)
        self.assertEqual(event_high['ambiente_id'], 'D')
        self.assertEqual(event_high['accion'], 'ventilacion_preventiva_sugerida')
        self.assertEqual(event_high['tipo'], 'intervencion.registrada')
        self.assertEqual(event_high['origen'], 'pronostico_automatico')
        self.assertEqual(event_high['humedad_predicha'], 45.5)
        self.assertEqual(snapshot(self.db)['interventions_emitted'], 1)

        # Subsequent minute within cooldown (issued 195 vs last_emitted 135 -> diff 60s < 300s) -> suppressed
        event_suppressed = evaluate_intervention(self.db, 'D', 120, 46.0, 195, upper_threshold=42.0, lower_threshold=30.0, cooldown_seconds=300)
        self.assertIsNone(event_suppressed)
        self.assertEqual(snapshot(self.db)['interventions_emitted'], 1)
        self.assertEqual(snapshot(self.db)['interventions_suppressed'], 1)

        # Replay of minute 120 -> idempotent (returns None without double counting suppression)
        replay = evaluate_intervention(self.db, 'D', 120, 46.0, 195, upper_threshold=42.0, lower_threshold=30.0, cooldown_seconds=300)
        self.assertIsNone(replay)

        # Minute after cooldown expired (issued 500 vs last_emitted 135 -> diff 365s > 300s) -> triggers new event
        event_after = evaluate_intervention(self.db, 'D', 420, 44.0, 500, upper_threshold=42.0, lower_threshold=30.0, cooldown_seconds=300)
        self.assertIsNotNone(event_after)
        self.assertEqual(snapshot(self.db)['interventions_emitted'], 2)
if __name__=='__main__':unittest.main(verbosity=2)
