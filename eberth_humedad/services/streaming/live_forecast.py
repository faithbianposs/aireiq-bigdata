"""Continuous MLlib inference on real minute events; no historical replay counted as anticipation."""
import json
import os
import time
from datetime import timezone,datetime
from pathlib import Path
from urllib.request import Request,urlopen
from pyspark.sql import SparkSession,functions as F,types as T
from pyspark.ml import PipelineModel
from kafka import KafkaProducer
from live_state import connect,record,snapshot,evaluate_intervention

_producer=None
def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer=KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:9092'),
                                    acks='all',retries=3,max_in_flight_requests_per_connection=1)
        except Exception as exc:
            print(json.dumps({'kafka_producer_warning':str(exc)}),flush=True)
            _producer=None
    return _producer

def publish_intervention(event):
    p=get_producer()
    if p is None: return None
    try:
        data=json.dumps(event,ensure_ascii=False).encode('utf-8')
        m=p.send('aireiq-intervenciones-v1',key=event['ambiente_id'].encode(),value=data).get(timeout=10)
        return m
    except Exception as exc:
        print(json.dumps({'kafka_publish_error':str(exc)}),flush=True)
        return None

root=Path('/evidence');out=root/'live_forecast';out.mkdir(parents=True,exist_ok=True)
pin=out/'model.json'
if pin.exists(): source=root/'s10_h120'/json.loads(pin.read_text())['run_id']
else:
    candidates=sorted(p for p in (root/'s10_h120').iterdir() if (p/'manifest.json').exists() and (p/'modelo').exists())
    assert candidates,'Entrenar horizonte de 120 segundos primero'
    source=candidates[-1]
training=json.loads((source/'manifest.json').read_text())
assert training['horizonte_segundos']==120 and training['status']=='trained_and_verified'
if not pin.exists():pin.write_text(json.dumps({'run_id':source.name}),encoding='utf-8')
db_path=str(out/'evaluation.sqlite');connect(db_path).close()
spark=(SparkSession.builder.appName('AireIQ-Pronostico-Vivo-120s').config('spark.sql.session.timeZone','UTC')
       .config('spark.sql.shuffle.partitions','2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
model=PipelineModel.load(str(source/'modelo'))
schema=T.StructType([T.StructField('schema_version',T.IntegerType()),
    *[T.StructField(c,T.StringType()) for c in ['event_id','dispositivo','origen']],
    T.StructField('minuto',T.TimestampType()),T.StructField('available_at',T.TimestampType()),
    T.StructField('replay',T.BooleanType()),T.StructField('lecturas',T.IntegerType()),
    *[T.StructField(c,T.DoubleType()) for c in ['humedad_pct','temperatura_c','presion_hpa']]])
raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:9092'))
     .option('subscribe','aireiq-minutos-vivos-v1').option('startingOffsets','earliest')
     .option('maxOffsetsPerTrigger',100).option('failOnDataLoss','true').load())
parsed=raw.select(F.col('key').cast('string').alias('key'),F.from_json(F.col('value').cast('string'),schema).alias('e')).select('key','e.*')
valid=parsed.filter((F.col('key')==F.col('dispositivo'))&(F.col('schema_version')==1)&
    (F.col('origen')=='supabase_live_minute')&(F.col('replay')==False)&(F.col('lecturas')>0)&
    F.col('minuto').isNotNull()&F.col('available_at').isNotNull()&F.col('event_id').isNotNull()&
    F.col('humedad_pct').between(0,100)&F.col('temperatura_c').between(-40,85)&F.col('presion_hpa').between(300,1100))

DISCARDED_TIMING=0  # minutos descartados por tiempo inválido o llegada tardía desde que arrancó el servicio

def consume(batch,batch_id):
    global DISCARDED_TIMING
    db=connect(db_path);accepted=0;rejected=0;interventions_sent=0
    try:
        for row in model.transform(batch).toLocalIterator():
            minute=int(row.minuto.replace(tzinfo=timezone.utc).timestamp())
            available=row.available_at.replace(tzinfo=timezone.utc).timestamp()
            issued=time.time()
            if minute%60 or available<minute+60 or available>issued+5:
                rejected+=1;continue
            record(db,row.dispositivo,minute,row.humedad_pct,available,row.lecturas,row.prediction,issued,source.name)
            accepted+=1
            if issued < (minute + 120):
                ev = evaluate_intervention(db, row.dispositivo, minute, row.prediction, issued)
                if ev:
                    m = publish_intervention(ev)
                    if m:
                        interventions_sent += 1
                        print(json.dumps({'intervention_emitted': ev['event_id'], 'action': ev['accion'],
                                          'prediction': ev['humedad_predicha'], 'topic': m.topic,
                                          'partition': m.partition, 'offset': m.offset}), flush=True)
        print(json.dumps({'batch_id':batch_id,'processed_rows':accepted,'invalid_timing':rejected,
                          'interventions_sent':interventions_sent}),flush=True)
    finally:
        DISCARDED_TIMING+=rejected
        db.close()

q=(valid.writeStream.foreachBatch(consume).option('checkpointLocation',str(out/'checkpoint'))
   .trigger(processingTime='5 seconds').start())

def report(active):
    db=connect(db_path)
    try:values=snapshot(db)
    finally:db.close()
    values.update(active=int(active),updated_timestamp=time.time(),batch_test_rmse=training['test']['rmse'],
                  discarded_timing=DISCARDED_TIMING)
    status=dict(model_run=source.name,model=training['modelo_elegido'],horizon_seconds=120,
                updated_at=datetime.now(timezone.utc).isoformat(),values=values,
                note='Error sobre agregados vivos posiblemente incompletos; solo predicciones anteriores al inicio objetivo.')
    lines=[]
    for name,value in values.items():lines.extend([f'# TYPE aireiq_live_{name} gauge',f'aireiq_live_{name} {value}'])
    try:
        req=Request('http://pushgateway:9091/metrics/job/aireiq_live',data=('\n'.join(lines)+'\n').encode(),
                    headers={'Content-Type':'text/plain; version=0.0.4'},method='PUT')
        with urlopen(req,timeout=5) as response:assert response.status in (200,202)
        status['metrics_published']=True
    except Exception as exc:status['metrics_published']=False;status['metrics_error_type']=type(exc).__name__
    temp=out/'status.tmp';temp.write_text(json.dumps(status,indent=2),encoding='utf-8');temp.replace(out/'status.json')

try:
    while q.isActive:
        report(True);q.awaitTermination(5)
    if q.exception():raise RuntimeError(str(q.exception()))
finally:
    q.stop();report(False);spark.stop()
    if _producer is not None:
        try:_producer.close()
        except Exception:pass
