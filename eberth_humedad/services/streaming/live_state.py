"""Durable evaluation: exact target minute, immutable observations, timely predictions and derived business events."""
import math
import os
import sqlite3
import uuid
from datetime import datetime, timezone

UPPER_THRESHOLD = float(os.environ.get('AIREIQ_ALERT_UPPER_HUMIDITY', '42.0'))
LOWER_THRESHOLD = float(os.environ.get('AIREIQ_ALERT_LOWER_HUMIDITY', '30.0'))
COOLDOWN_SECONDS = int(os.environ.get('AIREIQ_ALERT_COOLDOWN_SECONDS', '300'))

def connect(path):
    db=sqlite3.connect(path,timeout=30)
    db.executescript('''
    CREATE TABLE IF NOT EXISTS observations(device TEXT, minute INTEGER, actual REAL, available REAL,
      readings INTEGER, PRIMARY KEY(device,minute));
    CREATE TABLE IF NOT EXISTS predictions(device TEXT, minute INTEGER, target INTEGER, prediction REAL,
      baseline REAL, issued REAL, timely INTEGER, model_run TEXT, PRIMARY KEY(device,minute));
    CREATE TABLE IF NOT EXISTS interventions(device TEXT, minute INTEGER, target INTEGER, prediction REAL,
      threshold REAL, rule TEXT, action TEXT, event_id TEXT, issued REAL, emitted_at REAL, status TEXT,
      PRIMARY KEY(device,minute));
    ''')
    return db

def record(db,device,minute,actual,available,readings,prediction,issued,model_run):
    assert minute%60==0 and available>=minute+60 and available<=issued+5
    assert readings>0 and all(math.isfinite(x) for x in (actual,prediction,issued,available))
    target=minute+120
    with db:
        db.execute('INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?)',
                   (device,minute,actual,available,readings))
        db.execute('INSERT OR IGNORE INTO predictions VALUES (?,?,?,?,?,?,?,?)',
                   (device,minute,target,prediction,actual,issued,int(issued<target),model_run))

def evaluate_intervention(db,device,minute,prediction,issued,
                          upper_threshold=UPPER_THRESHOLD,
                          lower_threshold=LOWER_THRESHOLD,
                          cooldown_seconds=COOLDOWN_SECONDS):
    assert minute%60==0 and all(math.isfinite(x) for x in (prediction,issued))
    target=minute+120
    if prediction > upper_threshold:
        rule='humedad_alta_pronosticada'
        action='ventilacion_preventiva_sugerida'
        threshold=upper_threshold
    elif prediction < lower_threshold:
        rule='humedad_baja_pronosticada'
        action='mantenimiento_registrado'
        threshold=lower_threshold
    else:
        return None

    last_emitted=db.execute("SELECT max(emitted_at) FROM interventions WHERE device=? AND status='emitted'",
                            (device,)).fetchone()[0]

    with db:
        if last_emitted is not None and (issued - last_emitted) < cooldown_seconds:
            db.execute('INSERT OR IGNORE INTO interventions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                       (device,minute,target,prediction,threshold,rule,action,
                        None,issued,None,'suppressed_cooldown'))
            return None

        event_id=str(uuid.uuid4())
        cur=db.execute('INSERT OR IGNORE INTO interventions VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                       (device,minute,target,prediction,threshold,rule,action,
                        event_id,issued,issued,'emitted'))
        if not cur.rowcount:
            return None

    event={
        'event_id': event_id,
        'schema_version': 1,
        'occurred_at': datetime.fromtimestamp(issued, timezone.utc).isoformat(),
        'ambiente_id': device,
        'tipo': 'intervencion.registrada',
        'accion': action,
        'origen': 'pronostico_automatico',
        'humedad_predicha': round(prediction, 4),
        'umbral': threshold,
        'horizonte_segundos': 120,
        'regla': rule,
        'motivo': f'Humedad predicha {prediction:.2f}% a t+120s excede umbral {threshold:.2f}%',
    }
    return event

def snapshot(db):
    observed=db.execute('SELECT count(*) FROM observations').fetchone()[0]
    timely=db.execute('SELECT count(*) FROM predictions WHERE timely=1').fetchone()[0]
    scores=db.execute('''SELECT p.prediction,p.baseline,o.actual FROM predictions p JOIN observations o
                       ON p.device=o.device AND p.target=o.minute
                       WHERE p.timely=1 AND o.available>p.issued''').fetchall()
    values=dict(observations=observed,timely_predictions=timely,late_predictions=observed-timely,scored=len(scores))
    if scores:
        values.update(rmse=math.sqrt(sum((p-a)**2 for p,b,a in scores)/len(scores)),
                      mae=sum(abs(p-a) for p,b,a in scores)/len(scores),
                      baseline_rmse=math.sqrt(sum((b-a)**2 for p,b,a in scores)/len(scores)))
    obs=db.execute('SELECT actual,minute,readings FROM observations ORDER BY minute DESC LIMIT 1').fetchone()
    if obs: values.update(humidity_actual=obs[0],event_timestamp=obs[1],readings_in_minute=obs[2])
    pred=db.execute('SELECT prediction,target,issued,target-issued FROM predictions WHERE timely=1 ORDER BY issued DESC LIMIT 1').fetchone()
    if pred: values.update(humidity_prediction=pred[0],target_timestamp=pred[1],issued_timestamp=pred[2],lead_seconds=pred[3])
    emitted=db.execute("SELECT count(*) FROM interventions WHERE status='emitted'").fetchone()[0]
    suppressed=db.execute("SELECT count(*) FROM interventions WHERE status='suppressed_cooldown'").fetchone()[0]
    values.update(interventions_emitted=emitted,interventions_suppressed=suppressed)
    return values
