"""Prueba S10: inferencia stateless con MLlib sobre minutos históricos de Kafka."""
import json
import math
import os
import uuid
from datetime import datetime,timezone
from pathlib import Path
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient,NewTopic
from pyspark.sql import SparkSession,functions as F,types as T
from pyspark.ml import PipelineModel

spark=(SparkSession.builder.appName('AireIQ-S10-Inferencia-Kafka')
       .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
root=Path('/evidence')
horizon=int(os.environ.get('FORECAST_HORIZON_SECONDS','60'))
assert horizon in (60,120), 'Horizonte no validado'
experiment='s10' if horizon==60 else 's10_h120'
sources=sorted(p for p in (root/experiment).iterdir() if (p/'manifest.json').exists() and (p/'modelo').exists())
assert sources, 'Entrenar S10 primero'
source=sources[-1]
training=json.loads((source/'manifest.json').read_text())
assert training['horizonte_segundos']==horizon
run=uuid.uuid4().hex
topic='aireiq-s10-minutos-'+run[:12]
out=root/(experiment+'_streaming')/run;out.mkdir(parents=True)
bootstrap=os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
admin=KafkaAdminClient(bootstrap_servers=bootstrap);admin.create_topics([NewTopic(topic,1,1)]);admin.close()
producer=KafkaProducer(bootstrap_servers=bootstrap,acks='all',retries=3)
test=spark.read.parquet(str(source/'test_pairs')).orderBy('minuto').limit(100).cache()
assert test.count()>0
def iso(value):
    return value.replace(tzinfo=timezone.utc).isoformat()
for row in test.collect():
    # No se publica label ni target_time: solo la observación disponible en t.
    e=dict(event_id=str(uuid.uuid4()),schema_version=1,dispositivo=row.dispositivo,
           minuto=iso(row.minuto),humedad_pct=row.humedad_pct,temperatura_c=row.temperatura_c,
           presion_hpa=row.presion_hpa,replay=True,replay_run_id=run,origen='gold_historico_prueba')
    producer.send(topic,key=row.dispositivo.encode(),value=json.dumps(e).encode()).get(timeout=30)
producer.close()
print('Publicados minutos históricos de prueba:',test.count(),flush=True)
schema=T.StructType([T.StructField('schema_version',T.IntegerType()),
    *[T.StructField(c,T.StringType()) for c in ['event_id','dispositivo','replay_run_id','origen']],
    T.StructField('minuto',T.TimestampType()),T.StructField('replay',T.BooleanType()),
    *[T.StructField(c,T.DoubleType()) for c in ['humedad_pct','temperatura_c','presion_hpa']]])
raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',bootstrap)
     .option('subscribe',topic).option('startingOffsets','earliest').option('failOnDataLoss','true').load())
events=raw.select(F.col('key').cast('string').alias('key'),F.from_json(F.col('value').cast('string'),schema).alias('e')).select('key','e.*')
valid=events.filter((F.col('schema_version')==1)&(F.col('key')==F.col('dispositivo'))&F.col('minuto').isNotNull()&
                    F.col('event_id').isNotNull()&F.col('humedad_pct').between(0,100)&
                    F.col('temperatura_c').between(-40,85)&F.col('presion_hpa').between(300,1100))
model=PipelineModel.load(str(source/'modelo'))
result=(model.transform(valid).withColumn('target_time',F.col('minuto')+F.expr(f'INTERVAL {horizon} SECONDS'))
        .withColumn('model_run',F.lit(source.name)).withColumn('persistencia',F.col('humedad_pct'))
        .select('event_id','dispositivo','minuto','target_time','prediction','persistencia','model_run','replay_run_id','replay','origen'))
q=(result.writeStream.format('parquet').outputMode('append').option('path',str(out/'predicciones'))
   .option('checkpointLocation',str(out/'checkpoint')).trigger(availableNow=True).start())
try:
    q.awaitTermination()
    stored=spark.read.parquet(str(out/'predicciones')).cache()
    assert stored.count()==test.count()
    assert stored.filter(F.col('target_time').cast('long')-F.col('minuto').cast('long')!=horizon).count()==0
    expected=model.transform(test).select('dispositivo','minuto',F.col('prediction').alias('batch_prediction'),'label')
    compared=stored.join(expected,['dispositivo','minuto'])
    assert compared.count()==test.count()
    maximum=compared.agg(F.max(F.abs(F.col('prediction')-F.col('batch_prediction')))).first()[0]
    assert maximum is not None and maximum<=1e-9
    error=compared.agg(F.sqrt(F.avg(F.pow(F.col('prediction')-F.col('label'),2))).alias('rmse'),
                       F.avg(F.abs(F.col('prediction')-F.col('label'))).alias('mae')).first().asDict()
    report=dict(status='passed',run_id=run,model_run=source.name,modelo=training['modelo_elegido'],topic=topic,
                source='gold_historico_prueba',sensor_en_vivo=False,rows=stored.count(),horizon_seconds=horizon,
                max_difference_batch_stream=maximum,error_retrospectivo_subset=error,
                last_progress=list(q.recentProgress),online_error_measured=False,
                completed_at=datetime.now(timezone.utc).isoformat())
    (out/'manifest.json').write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='last_progress'},indent=2),flush=True)
finally:
    q.stop();spark.stop()
