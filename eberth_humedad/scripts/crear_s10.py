from pathlib import Path
import nbformat as n

nb=n.v4.new_notebook()
nb.metadata.kernelspec=dict(display_name='Python 3 (ipykernel)',language='python',name='python3')
cells=[]
def md(s): cells.append(n.v4.new_markdown_cell(s))
def code(s): cells.append(n.v4.new_code_cell(s))
md('''# S10 Pronóstico de humedad a un minuto
**AireIQ — Eberth Emmanuel Rosales Castillo.**

Pregunta: ¿cuál será la humedad media del próximo minuto, usando solo la lectura agregada del minuto actual?

Se utiliza Gold de S3 y se crean pares solo cuando existen dos minutos consecutivos del mismo dispositivo y tramo. No se atraviesan huecos ni se usa como etiqueta «la siguiente fila» cuando no está exactamente a 60 segundos.

Persistencia predice `humedad(t+1) = humedad(t)`. Se compara contra regresión lineal y Random Forest. Separación temporal 60/20/20; se excluyen pares que cruzan cortes. Se elige por RMSE de validación incluyendo la persistencia y se evalúa al ganador en prueba. No se presupone una mejora.

La entrada del modelo es una sola observación agregada por minuto. No necesita historial en memoria para `transform`. Esto no significa aplicar un modelo entrenado con promedios a lecturas instantáneas crudas: el contrato de inferencia mantiene la misma granularidad.
''')
code('''from pathlib import Path
from datetime import datetime, timezone
import json, math
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
spark=(SparkSession.builder.appName("AireIQ-S10-Pronostico").master("local[2]")
       .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","2").getOrCreate())
root=Path('/evidence') if Path('/evidence').exists() else Path('/opt/aireiq/outputs')
sources=sorted(p for p in (root/'s03').iterdir() if (p/'manifest.json').exists() and (p/'gold/_SUCCESS').exists())
assert sources
source=sources[-1]
source_manifest=json.loads((source/'manifest.json').read_text())
out=root/'s10'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
out.mkdir(parents=True,exist_ok=False)
data=spark.read.parquet(str(source/'gold'))
w=Window.partitionBy('dispositivo','tramo').orderBy('minuto')
with_next=(data.withColumn('target_time',F.lead('minuto').over(w))
    .withColumn('label',F.lead('humedad_pct').over(w)))
pairs=with_next.filter(F.col('target_time').cast('long')-F.col('minuto').cast('long')==60).cache()
assert pairs.count()>100
times=[r.minuto for r in pairs.select('minuto').distinct().orderBy('minuto').collect()]
cut1,cut2=times[int(len(times)*.6)],times[int(len(times)*.8)]
train=pairs.filter(F.col('target_time')<F.lit(cut1)).cache()
validation=pairs.filter((F.col('minuto')>=F.lit(cut1))&(F.col('target_time')<F.lit(cut2))).cache()
test=pairs.filter(F.col('minuto')>=F.lit(cut2)).cache()
counts={k:v.count() for k,v in [('train',train),('validation',validation),('test',test)]}
assert min(counts.values())>0
assert train.agg(F.max('target_time')).first()[0]<validation.agg(F.min('minuto')).first()[0]
assert validation.agg(F.max('target_time')).first()[0]<test.agg(F.min('minuto')).first()[0]
print('Minutos Gold:',data.count(),'Pares consecutivos:',pairs.count(),'Bloques:',counts)
print('Pares excluidos en cortes:',pairs.count()-sum(counts.values()))
''')
code('''features=['humedad_pct','temperatura_c','presion_hpa','hora_sin','hora_cos']
time_sql="SELECT *, sin(2*PI()*(hour(from_utc_timestamp(minuto,'America/Lima'))+minute(minuto)/60.0)/24) AS hora_sin, cos(2*PI()*(hour(from_utc_timestamp(minuto,'America/Lima'))+minute(minuto)/60.0)/24) AS hora_cos FROM __THIS__"
def model(name):
    if name=='persistencia':
        return Pipeline(stages=[SQLTransformer(statement='SELECT *, humedad_pct AS prediction FROM __THIS__')])
    estimator={
        'lineal':LinearRegression(labelCol='label',regParam=.1,maxIter=100),
        'random_forest':RandomForestRegressor(labelCol='label',numTrees=50,maxDepth=6,seed=42)
    }[name]
    return Pipeline(stages=[SQLTransformer(statement=time_sql),VectorAssembler(inputCols=features,outputCol='features',handleInvalid='error'),estimator])
def metrics(pred):
    result={m:RegressionEvaluator(labelCol='label',predictionCol='prediction',metricName=m).evaluate(pred) for m in ['rmse','mae','r2']}
    assert all(math.isfinite(v) for v in result.values())
    return result
results=[]
for name in ['persistencia','lineal','random_forest']:
    fitted=model(name).fit(train)
    pred=fitted.transform(validation).cache()
    score=metrics(pred); results.append({'modelo':name,**score})
    print(name,score,flush=True);pred.unpersist()
# Desempate a favor de persistencia, evitando complejidad sin mejora.
winner=min(results,key=lambda r:(r['rmse'],r['modelo']!='persistencia'))['modelo']
spark.createDataFrame(results).select('modelo','rmse','mae','r2').orderBy('rmse').show()
print('Elegido por validación:',winner)
''')
md('''## Prueba independiente y persistencia del pipeline
El test permanece fuera de la selección. Si un modelo gana en validación y pierde en prueba, se reporta sin cambiar el ganador retrospectivamente. El pipeline completo se guarda y se recarga antes de usarlo en streaming.
''')
code('''development=train.unionByName(validation).cache()
final_model=model(winner).fit(development)
pred=final_model.transform(test).cache()
test_score=metrics(pred)
baseline_score=metrics(test.withColumn('prediction',F.col('humedad_pct')))
final_model.write().save(str(out/'modelo'))
loaded=PipelineModel.load(str(out/'modelo'))
again=loaded.transform(test).select('dispositivo','minuto',F.col('prediction').alias('restored'))
diff=pred.select('dispositivo','minuto','prediction').join(again,['dispositivo','minuto']).agg(F.max(F.abs(F.col('prediction')-F.col('restored')))).first()[0]
assert diff is not None and diff<=1e-9
test.select('dispositivo','minuto','target_time','humedad_pct','temperatura_c','presion_hpa','label').write.parquet(str(out/'test_pairs'))
pred.select('dispositivo','minuto','target_time','humedad_pct','label','prediction').write.parquet(str(out/'predicciones_test'))
relative=100*(baseline_score['rmse']-test_score['rmse'])/baseline_score['rmse'] if baseline_score['rmse'] else None
report=dict(status='trained_and_verified',s03_run=source.name,source_sha256=source_manifest['source_sha256'],
    run_id=out.name,objetivo='humedad media del siguiente minuto',horizonte_segundos=60,
    unidad_entrada='promedio por minuto',spark_version=spark.version,timezone='UTC; hora ciclica America/Lima',
    total_minutos=data.count(),pares_consecutivos=pairs.count(),conteos=counts,
    pares_cruzando_cortes_excluidos=pairs.count()-sum(counts.values()),features=features,
    validacion=results,modelo_elegido=winner,test=test_score,baseline_test=baseline_score,
    mejora_rmse_test_porcentaje=relative,diferencia_recarga=diff,
    test_start=str(test.agg(F.min('minuto')).first()[0]),test_end=str(test.agg(F.max('target_time')).first()[0]))
(out/'manifest.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
print(json.dumps(report,indent=2,ensure_ascii=False))
print('Modelo y evidencia:',out)
for frame in [pairs,train,validation,test,development,pred]:frame.unpersist()
''')
md('''## Límites de la evaluación
RMSE y MAE se expresan en puntos porcentuales. La prueba no representa otros ambientes o dispositivos. La reproducción de Kafka utilizará observaciones históricas de prueba, con marca de replay; no es pronóstico del sensor en vivo. El error contra la etiqueta histórica es verificación retrospectiva; medir error al llegar una observación futura en operación corresponde a la integración posterior.

La ruta de inferencia no debe esperar las ventanas finalizadas por watermark de S8: para un horizonte de un minuto, ese retraso de dos minutos haría llegar tarde el pronóstico. Se mantiene un contrato de minutos cerrados independiente del sink de ventanas tardías. El adaptador continuo de minutos sigue pendiente.
''')
nb.cells=cells;n.validate(nb)
target=Path(__file__).resolve().parents[1]/'notebooks/10_pronostico_humedad.ipynb'
n.write(nb,target);print(target)
