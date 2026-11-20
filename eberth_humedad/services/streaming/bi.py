"""S11: replay ordenado, inferencia y evaluación al llegar el valor observado."""
import json
import math
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime,timezone
from pathlib import Path
from urllib.request import Request,urlopen
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient,NewTopic
from pyspark.sql import SparkSession,functions as F,types as T
from pyspark.ml import PipelineModel

spark=(SparkSession.builder.appName('AireIQ-S11-BI-Humedad').config('spark.sql.session.timeZone','UTC')
       .config('spark.sql.shuffle.partitions','2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
root=Path('/evidence')
source=sorted(p for p in (root/'s10').iterdir() if (p/'manifest.json').exists())[-1]
train=json.loads((source/'manifest.json').read_text())
u1_path=sorted((root/'s04').glob('*/manifest.json'))[-1]
u1=json.loads(u1_path.read_text())
run=uuid.uuid4().hex
topic='aireiq-s11-'+run[:12]
out=root/'s11'/run;out.mkdir(parents=True)
db_path=out/'evaluacion.sqlite'
con=sqlite3.connect(db_path)
con.executescript('''
CREATE TABLE observations(event_id TEXT PRIMARY KEY, device TEXT, ts TEXT, actual REAL, arrived REAL);
CREATE TABLE predictions(event_id TEXT PRIMARY KEY, device TEXT, target TEXT, prediction REAL, baseline REAL);
CREATE TABLE scores(device TEXT, target TEXT, prediction REAL, baseline REAL, actual REAL, PRIMARY KEY(device,target));
''')
con.close()
model=PipelineModel.load(str(source/'modelo'))
rows=spark.read.parquet(str(source/'test_pairs')).orderBy('minuto').limit(60).collect()
expected_times={r.minuto for r in rows}
expected_scores=sum(1 for r in rows if r.target_time in expected_times)
assert expected_scores>0
bootstrap=os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
admin=KafkaAdminClient(bootstrap_servers=bootstrap);admin.create_topics([NewTopic(topic,1,1)]);admin.close()
schema=T.StructType([T.StructField('event_id',T.StringType()),T.StructField('dispositivo',T.StringType()),
    T.StructField('minuto',T.TimestampType()),T.StructField('sent_at',T.DoubleType()),
    *[T.StructField(c,T.DoubleType()) for c in ['humedad_pct','temperatura_c','presion_hpa']]])
raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',bootstrap)
     .option('subscribe',topic).option('startingOffsets','earliest').load())
parsed=raw.select('offset',F.from_json(F.col('value').cast('string'),schema).alias('e')).select('offset','e.*')
valid=parsed.filter(F.col('minuto').isNotNull()&F.col('event_id').isNotNull()&F.col('humedad_pct').between(0,100)&
                    F.col('temperatura_c').between(-40,85)&F.col('presion_hpa').between(300,1100))
predicted=model.transform(valid).withColumn('target_time',F.col('minuto')+F.expr('INTERVAL 1 MINUTE'))
latest={}
def push(active):
    con=sqlite3.connect(db_path)
    count=con.execute('SELECT count(*) FROM observations').fetchone()[0]
    scores=con.execute('SELECT prediction,baseline,actual FROM scores').fetchall()
    con.close()
    lines=[]
    def metric(name,value):
        assert math.isfinite(float(value))
        lines.extend([f'# TYPE {name} gauge',f'{name} {float(value)}'])
    metric('aireiq_bi_replay',1);metric('aireiq_bi_active',active);metric('aireiq_bi_updated_timestamp',time.time())
    metric('aireiq_bi_observations',count);metric('aireiq_bi_scored',len(scores))
    metric('aireiq_u1_rmse',u1['test']['rmse']);metric('aireiq_u1_baseline_rmse',u1['baseline_test']['rmse'])
    metric('aireiq_s10_test_rmse',train['test']['rmse']);metric('aireiq_s10_baseline_rmse',train['baseline_test']['rmse'])
    metric('aireiq_humidity_p5',u1['p5']);metric('aireiq_humidity_p95',u1['p95'])
    if latest:
        for k,v in latest.items():metric(k,v)
    if scores:
        metric('aireiq_bi_rmse',math.sqrt(sum((p-a)**2 for p,b,a in scores)/len(scores)))
        metric('aireiq_bi_mae',sum(abs(p-a) for p,b,a in scores)/len(scores))
        metric('aireiq_bi_baseline_rmse',math.sqrt(sum((b-a)**2 for p,b,a in scores)/len(scores)))
    req=Request('http://pushgateway:9091/metrics/job/aireiq_bi',data=('\n'.join(lines)+'\n').encode(),
                headers={'Content-Type':'text/plain; version=0.0.4'},method='PUT')
    with urlopen(req,timeout=10) as response:assert response.status in [200,202]

def consume(batch,batch_id):
    con=sqlite3.connect(db_path)
    # Orden de llegada Kafka, una partición de demostración. No leer etiqueta futura.
    for r in batch.orderBy('offset').toLocalIterator():
        ts=r.minuto.isoformat();target=r.target_time.isoformat()
        with con:
            if con.execute('SELECT 1 FROM observations WHERE event_id=?',(r.event_id,)).fetchone():continue
            pending=con.execute('SELECT prediction,baseline FROM predictions WHERE device=? AND target=?',
                                (r.dispositivo,ts)).fetchone()
            if pending:
                con.execute('INSERT OR IGNORE INTO scores VALUES(?,?,?,?,?)',(r.dispositivo,ts,*pending,r.humedad_pct))
                latest['aireiq_bi_prediction_for_actual']=pending[0]
                latest['aireiq_bi_last_absolute_error']=abs(pending[0]-r.humedad_pct)
            con.execute('INSERT INTO observations VALUES(?,?,?,?,?)',(r.event_id,r.dispositivo,ts,r.humedad_pct,time.time()))
            con.execute('INSERT INTO predictions VALUES(?,?,?,?,?)',(r.event_id,r.dispositivo,target,r.prediction,r.humedad_pct))
        latest.update(aireiq_bi_humidity_actual=r.humedad_pct,aireiq_bi_humidity_next=r.prediction,
                      aireiq_bi_event_timestamp=r.minuto.replace(tzinfo=timezone.utc).timestamp(),
                      aireiq_bi_target_timestamp=r.target_time.replace(tzinfo=timezone.utc).timestamp(),
                      aireiq_bi_processing_delay_seconds=max(0,time.time()-r.sent_at),
                      aireiq_bi_alert=int(r.prediction<u1['p5'] or r.prediction>u1['p95']))
    con.close()
    push(1)
    print('MICROBATCH EVALUADO',batch_id,flush=True)

q=(predicted.writeStream.foreachBatch(consume).option('checkpointLocation',str(out/'checkpoint'))
   .trigger(processingTime='3 seconds').start())
producer_error=[]
def publish():
    p=KafkaProducer(bootstrap_servers=bootstrap,acks='all')
    try:
        for i,r in enumerate(rows):
            e=dict(event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,run+':'+str(i))),dispositivo=r.dispositivo,
                   minuto=r.minuto.replace(tzinfo=timezone.utc).isoformat(),sent_at=time.time(),
                   humedad_pct=r.humedad_pct,temperatura_c=r.temperatura_c,presion_hpa=r.presion_hpa,
                   origen='replay_gold_s11',replay=True,replay_run_id=run)
            p.send(topic,key=r.dispositivo.encode(),value=json.dumps(e).encode()).get(timeout=30)
            if i%10==0:print('PUBLICADO',i+1,'de',len(rows),flush=True)
            time.sleep(.5)
    except Exception as exc:producer_error.append(str(exc))
    finally:p.close()

try:
    push(1)
    thread=threading.Thread(target=publish);thread.start()
    while thread.is_alive():
        if q.exception():raise RuntimeError(str(q.exception()))
        thread.join(timeout=2)
    assert not producer_error,producer_error
    q.processAllAvailable()
    con=sqlite3.connect(db_path)
    observed=con.execute('SELECT count(*) FROM observations').fetchone()[0]
    scored=con.execute('SELECT count(*) FROM scores').fetchone()[0]
    assert observed==len(rows) and scored==expected_scores,(observed,scored,expected_scores)
    errors=con.execute('SELECT prediction,baseline,actual FROM scores').fetchall();con.close()
    report=dict(status='passed',run_id=run,topic=topic,model_run=source.name,modelo=train['modelo_elegido'],
                input_events=observed,scored_on_arrival=scored,expected_scores=expected_scores,
                rmse=math.sqrt(sum((p-a)**2 for p,b,a in errors)/scored),
                baseline_rmse=math.sqrt(sum((b-a)**2 for p,b,a in errors)/scored),
                origen='reproduccion_historica_acelerada',sensor_en_vivo=False,
                evaluation='Prediccion guardada se evalua cuando llega la observacion de target_time',
                completed_at=datetime.now(timezone.utc).isoformat())
    (out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2),flush=True)
finally:
    q.stop();push(0);spark.stop()
