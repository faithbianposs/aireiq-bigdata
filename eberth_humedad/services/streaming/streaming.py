"""S8: Kafka -> deduplicación con watermark -> ventanas -> Parquet."""
import argparse
import json
import math
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'aireiq-mediciones-v1'
spark = (SparkSession.builder.appName('AireIQ-S8-Humedad')
         .config('spark.sql.session.timeZone','UTC')
         .config('spark.sql.shuffle.partitions','2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
schema = T.StructType([
    T.StructField('schema_version',T.IntegerType()),
    *[T.StructField(c,T.StringType()) for c in ['event_id','dispositivo','replay_run_id','origen']],
    T.StructField('event_time',T.TimestampType()),
    T.StructField('ingested_at',T.TimestampType()),
    T.StructField('replay',T.BooleanType()),
    *[T.StructField(c,T.DoubleType()) for c in ['humedad_pct','temperatura_c','presion_hpa','gas_ohms']]
])

def aggregated(topic, sliding=False):
    raw = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', BOOTSTRAP)
           .option('subscribe',topic).option('startingOffsets','earliest')
           .option('failOnDataLoss','true').load())
    parsed = raw.select(F.col('key').cast('string').alias('kafka_key'),
                        F.from_json(F.col('value').cast('string'),schema).alias('e')).select('kafka_key','e.*')
    valid = parsed.filter(
        (F.col('schema_version') == 1) & (F.col('kafka_key') == F.col('dispositivo')) &
        F.col('event_time').isNotNull() & F.col('ingested_at').isNotNull() &
        (F.length(F.trim('event_id')) > 0) & (F.length(F.trim('replay_run_id')) > 0) &
        (F.col('origen').isin('reproduccion_csv','prueba_controlada') |
         ((F.col('origen')=='supabase_poll') & (F.col('replay')==False))) &
        F.col('humedad_pct').between(0,100) & F.col('temperatura_c').between(-40,85) &
        F.col('presion_hpa').between(300,1100) & (F.col('gas_ohms') > 0) &
        (F.col('gas_ohms') < float('inf')))
    clean = (valid.withWatermark('event_time','2 minutes')
             .dropDuplicatesWithinWatermark(['replay_run_id','event_id']))
    window = F.window('event_time','5 minutes','1 minute') if sliding else F.window('event_time','1 minute')
    return (clean.groupBy('dispositivo','replay_run_id', window)
            .agg(F.count('*').alias('lecturas'),F.avg('humedad_pct').alias('humedad_media_pct'),
                 F.min('humedad_pct').alias('humedad_min_pct'),F.max('humedad_pct').alias('humedad_max_pct')))

def start(topic, path, finite=False, sliding=False):
    writer = (aggregated(topic, sliding).writeStream.format('parquet').outputMode('append')
              .option('path',str(path/'ventanas'))
              .option('checkpointLocation',str(path/'checkpoint')))
    if finite:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime='3 seconds')
    return writer.start()

def progress(q,path,label):
    values = list(q.recentProgress)
    (path/f'progress_{label}.json').write_text(json.dumps(values,indent=2,default=str),encoding='utf-8')
    print(json.dumps({'fase':label,'query_id':str(q.id),'run_id':str(q.runId),
                      'ultimo_progreso':q.lastProgress},default=str),flush=True)
    return values

def read_results(path):
    output = str(path/'ventanas')
    spark.catalog.refreshByPath(output)
    result_schema = 'dispositivo STRING, replay_run_id STRING, window STRUCT<start: TIMESTAMP, end: TIMESTAMP>, lecturas BIGINT, humedad_media_pct DOUBLE, humedad_min_pct DOUBLE, humedad_max_pct DOUBLE'
    return spark.read.schema(result_schema).parquet(output)

def test(sliding=False):
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    run = uuid.uuid4().hex
    topic = 'aireiq-s08-prueba-' + run[:12]
    path = Path('/evidence/s08_deslizante' if sliding else '/evidence/s08')/run
    path.mkdir(parents=True)
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(topic,1,1)])
    admin.close()
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,acks='all',retries=3)
    base = datetime.now(timezone.utc).replace(second=0,microsecond=0)
    def send(seconds, humidity, identity=None, gas=1000):
        e = dict(schema_version=1,event_id=identity or str(uuid.uuid4()),dispositivo='ESP32_TEST',
                 replay_run_id=run,origen='prueba_controlada',event_time=(base+timedelta(seconds=seconds)).isoformat(),
                 ingested_at=datetime.now(timezone.utc).isoformat(),humedad_pct=humidity,
                 temperatura_c=20,presion_hpa=645,gas_ohms=gas)
        m=producer.send(topic,key=b'ESP32_TEST',value=json.dumps(e).encode()).get(timeout=30)
        print('PUBLICADO',seconds,humidity,m.partition,m.offset,flush=True)
    q = start(topic,path,sliding=sliding)
    all_progress=[]
    try:
        # Dos lecturas válidas en la misma ventana, una repetida y una inválida.
        identity=str(uuid.uuid4())
        send(0,40,identity); send(0,40,identity); send(20,60); send(30,100,gas=0)
        q.processAllAvailable()
        all_progress += progress(q,path,'inicial')
        # Desplazar el máximo event_time; el watermark es max(event_time)-2 min.
        send(600,50)
        q.processAllAvailable()
        all_progress += progress(q,path,'avance')
        first = read_results(path).filter(F.col('window.start')==F.lit(base)).collect()
        assert len(first)==1 and first[0].lecturas==2 and math.isclose(first[0].humedad_media_pct,50), first
        if sliding:
            overlapping = read_results(path).filter(F.col('window.end')<=F.lit(base+timedelta(minutes=5))).collect()
            assert len(overlapping)==5, overlapping
            assert all(r.lecturas==2 and math.isclose(r.humedad_media_pct,50) for r in overlapping)
        # Evento antiguo en un microbatch posterior: ya está detrás del watermark.
        send(10,90)
        q.processAllAvailable()
        all_progress += progress(q,path,'tardio')
        dropped=sum(s.get('numRowsDroppedByWatermark',0) for p in q.recentProgress for s in p.get('stateOperators',[]))
        assert dropped >= 1, 'No hay evidencia de descarte por watermark'
        before = read_results(path).count()
        old_id=str(q.id); old_run=str(q.runId)
        q.stop()
        # Misma salida/checkpoint, nueva ejecución de la consulta.
        q=start(topic,path,sliding=sliding)
        q.processAllAvailable()
        assert str(q.id)==old_id and str(q.runId)!=old_run
        assert read_results(path).count()==before, 'Se duplicaron ventanas al reiniciar'
        send(1200,55)
        q.processAllAvailable()
        all_progress += progress(q,path,'recuperacion')
        second=read_results(path).filter(F.col('window.start')==F.lit(base+timedelta(minutes=10))).collect()
        assert len(second)==1 and second[0].lecturas==1 and math.isclose(second[0].humedad_media_pct,50), second
        first_again=read_results(path).filter(F.col('window.start')==F.lit(base)).collect()
        assert len(first_again)==1 and first_again[0].lecturas==2
        report=dict(status='passed',tipo='prueba_controlada',topic=topic,run_id=run,watermark='2 minutes',
                    ventana='5 minutes' if sliding else '1 minute',avance='1 minute',
                    solapamiento_verificado=sliding,deduplicacion_verificada=True,invalido_excluido=True,
                    tardios_descartados=dropped,checkpoint_query_id=old_id,
                    reinicio='consulta detenida y recreada con el mismo checkpoint',
                    primera_ventana_lecturas=2,primera_ventana_media=50,
                    ventanas_finalizadas=read_results(path).count(),
                    completed_at=datetime.now(timezone.utc).isoformat())
        (path/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report,indent=2),flush=True)
    finally:
        q.stop(); producer.close()

def history(supabase=False):
    run=uuid.uuid4().hex
    topic='aireiq-mediciones-supabase-v1' if supabase else TOPIC
    path=Path('/evidence/supabase_spark' if supabase else '/evidence/s08_historico')/run
    path.mkdir(parents=True)
    q=start(topic,path,finite=True)
    q.awaitTermination()
    values=progress(q,path,'historico')
    assert any(p['numInputRows']>0 for p in values), 'No se consumieron datos Kafka'
    count=read_results(path).count()
    assert count>0
    report=dict(status='passed',origen='supabase_poll' if supabase else 'reproduccion_csv',topic=topic,ventanas_finalizadas=count,
                input_rows=sum(p['numInputRows'] for p in values),
                nota='Las ultimas ventanas permanecen en estado hasta que avance el watermark.',run_id=run)
    (path/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2),flush=True)

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('mode',choices=['test','test-sliding','history','supabase-history','live']); args=ap.parse_args()
    try:
        if args.mode=='test': test()
        elif args.mode=='test-sliding': test(sliding=True)
        elif args.mode=='history': history()
        elif args.mode=='supabase-history': history(supabase=True)
        else:
            path=Path('/evidence/s08_live'); path.mkdir(parents=True,exist_ok=True)
            q=start(TOPIC,path)
            print('STREAMING ACTIVO: ventanas de 1 minuto, watermark de 2 minutos',flush=True)
            # Archivo atómico de progreso para el exporter de S9.
            previous = {}
            status_path = path/'status.json'
            if status_path.exists():
                previous = json.loads(status_path.read_text())
            last_data = previous.get('last_data_batch')
            try:
                while q.isActive:
                    latest = q.lastProgress
                    if latest and latest.get('numInputRows',0) > 0:
                        last_data = latest
                    status = dict(updated_at=datetime.now(timezone.utc).isoformat(),active=q.isActive,
                                  query_id=str(q.id),run_id=str(q.runId),status=q.status,
                                  progress=latest,last_data_batch=last_data)
                    temp=path/'status.tmp'
                    temp.write_text(json.dumps(status,default=str),encoding='utf-8')
                    temp.replace(status_path)
                    q.awaitTermination(5)
                if q.exception():
                    raise RuntimeError(str(q.exception()))
            finally:
                status = dict(updated_at=datetime.now(timezone.utc).isoformat(),active=False,
                              progress=q.lastProgress,last_data_batch=last_data)
                temp=path/'status.tmp'; temp.write_text(json.dumps(status,default=str),encoding='utf-8')
                temp.replace(status_path)
    finally:
        spark.stop()
