from pathlib import Path
import nbformat as n

nb = n.v4.new_notebook()
nb.metadata.kernelspec = dict(display_name='Python 3 (ipykernel)', language='python', name='python3')
cells = []
def md(s): cells.append(n.v4.new_markdown_cell(s))
def code(s): cells.append(n.v4.new_code_cell(s))
md('''# AireIQ · S4: regresión de humedad con MLlib
**Responsable: Eberth Emmanuel Rosales Castillo.**

Pregunta U1: ¿Qué tan precisamente se estima la humedad relativa usando temperatura, presión y hora del mismo instante?

Se utiliza Gold de S3, con una observación por minuto. Esta estimación contemporánea no es el pronóstico a un minuto de U2 y no demuestra causalidad.

Método: primer 60 % cronológico para entrenar, siguiente 20 % para seleccionar por RMSE y último 20 % para prueba final. Se preservan timestamps completos en cada corte. La separación temporal es una decisión propia frente al ejemplo aleatorio de U1: reduce la mezcla de mediciones vecinas entre entrenamiento y prueba. La prueba no elige hiperparámetros. Se vuelve a ajustar la configuración ganadora con el primer 80 % y se evalúa una vez en el último 20 %.

Se comparan LinearRegression, Ridge, Elastic Net y RandomForestRegressor, más una referencia constante (media de entrenamiento). RMSE y MAE se expresan en puntos porcentuales de humedad; R² puede ser negativo. No se presupone que los modelos superen la referencia.

Referencia: https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/
''')
code('''from pathlib import Path
from datetime import datetime, timezone
import json, math, hashlib
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = (SparkSession.builder.appName("AireIQ-S4-Humedad").master("local[2]")
    .config("spark.sql.session.timeZone", "America/Lima")
    .config("spark.sql.shuffle.partitions", "4").getOrCreate())
# Elegir solo ejecuciones S3 terminadas, con manifiesto y Parquet.
root = Path("/opt/aireiq/outputs")
sources = sorted(p for p in (root / "s03").iterdir() if (p / "manifest.json").is_file() and (p / "gold/_SUCCESS").is_file())
assert sources, "Ejecutar S3 primero"
source = sources[-1]
s03 = json.loads((source / "manifest.json").read_text())
assert s03["roundtrip_valores"] and s03["partition_filters"]
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
out = root / "s04" / run_id
out.mkdir(parents=True, exist_ok=False)
data = spark.read.parquet(str(source / "gold")).cache()
times = [r.minuto for r in data.select("minuto").distinct().orderBy("minuto").collect()]
assert len(times) >= 100, "Historial insuficiente para tres bloques"
cut1, cut2 = times[int(len(times)*.6)], times[int(len(times)*.8)]
train = data.filter(F.col("minuto") < F.lit(cut1)).cache()
validation = data.filter((F.col("minuto") >= F.lit(cut1)) & (F.col("minuto") < F.lit(cut2))).cache()
test = data.filter(F.col("minuto") >= F.lit(cut2)).cache()
counts = {"train": train.count(), "validation": validation.count(), "test": test.count()}
assert min(counts.values()) > 0 and sum(counts.values()) == data.count()
for left, right in [(train, validation), (validation, test)]:
    assert left.agg(F.max("minuto")).first()[0] < right.agg(F.min("minuto")).first()[0]
print("S3:", source.name, "Filas por bloque:", counts)
# Mostrar cortes como strings en zona de Lima; internamente los timestamps son instantes UTC.
for label, frame in [("train", train), ("validation", validation), ("test", test)]:
    print(label, frame.select(F.min("minuto").cast("string"), F.max("minuto").cast("string")).first())
''')
md('''## Preparación y comparación
La hora se representa con seno y coseno, de modo que 23:59 y 00:00 sean cercanas. Estas transformaciones y VectorAssembler forman parte del modelo guardado. No se usan humedad actual ni índices derivados como predictores del objetivo contemporáneo.
''')
code('''features = ["temperatura_c", "presion_hpa", "hora_sin", "hora_cos"]
sql = "SELECT *, sin(2 * PI() * (hour(minuto) + minute(minuto)/60.0) / 24) AS hora_sin, cos(2 * PI() * (hour(minuto) + minute(minuto)/60.0) / 24) AS hora_cos FROM __THIS__"
def pipeline(name):
    regressors = {
        "lineal": LinearRegression(labelCol="humedad_pct", regParam=0.0, maxIter=100),
        "ridge": LinearRegression(labelCol="humedad_pct", regParam=0.1, elasticNetParam=0.0, maxIter=100),
        "elastic_net": LinearRegression(labelCol="humedad_pct", regParam=0.1, elasticNetParam=0.5, maxIter=100),
        "random_forest": RandomForestRegressor(labelCol="humedad_pct", numTrees=50, maxDepth=6, seed=42)
    }
    return Pipeline(stages=[SQLTransformer(statement=sql), VectorAssembler(inputCols=features, outputCol="features", handleInvalid="error"), regressors[name]])
def metrics(pred):
    result = {m: RegressionEvaluator(labelCol="humedad_pct", predictionCol="prediction", metricName=m).evaluate(pred) for m in ["rmse", "mae", "r2"]}
    assert all(math.isfinite(v) for v in result.values()), result
    return result

mean_train = train.agg(F.avg("humedad_pct")).first()[0]
baseline_val = metrics(validation.withColumn("prediction", F.lit(mean_train)))
results = []
for name in ["lineal", "ridge", "elastic_net", "random_forest"]:
    fitted = pipeline(name).fit(train)
    pred = fitted.transform(validation).cache()
    score = metrics(pred)
    results.append({"modelo": name, **score})
    pred.unpersist()
    print(name, score, flush=True)
winner = min(results, key=lambda r: (r["rmse"], r["modelo"]))["modelo"]
spark.createDataFrame(results + [{"modelo":"referencia_media", **baseline_val}]).select("modelo", "rmse", "mae", "r2").orderBy("rmse").show(truncate=False)
print("Mejor configuración ML en validación:", winner)
''')
code('''development = train.unionByName(validation).cache()
final_model = pipeline(winner).fit(development)
pred_test = final_model.transform(test).cache()
test_metrics = metrics(pred_test)
mean_dev = development.agg(F.avg("humedad_pct")).first()[0]
baseline_test = metrics(test.withColumn("prediction", F.lit(mean_dev)))
final_model.write().save(str(out / "modelo_humedad_u1"))
loaded = PipelineModel.load(str(out / "modelo_humedad_u1"))
reloaded = loaded.transform(test).select("dispositivo", "minuto", F.col("prediction").alias("restored"))
delta = pred_test.select("dispositivo", "minuto", "prediction").join(reloaded, ["dispositivo", "minuto"]).select(F.max(F.abs(F.col("prediction")-F.col("restored")))).first()[0]
assert delta is not None and delta <= 1e-9, delta
assert pred_test.count() == test.count()
assert pred_test.filter(F.col("prediction").isNull() | F.isnan("prediction")).count() == 0
pred_test.select("dispositivo", "minuto", "fecha", "humedad_pct", "prediction").write.partitionBy("fecha").parquet(str(out / "predicciones_test"))
print("Prueba final:", test_metrics)
print("Referencia media:", baseline_test)
print("Supera referencia en RMSE:", test_metrics["rmse"] < baseline_test["rmse"])
print("Máxima diferencia tras recargar:", delta)
''')
md('''## Indicador descriptivo
Referencia estadística: percentiles p5/p95 de los minutos del primer 60 % del historial, calculados solo con entrenamiento. Este periodo es una decisión metodológica explícita, pendiente de aprobación académica; no se reutilizan los umbrales de IAQ del brief. El indicador solicitado se calcula sobre **lecturas válidas Silver**, con denominador por día/hora. Se entrega además una vista por minuto, que reduce el sesgo de frecuencia. Un porcentaje de lecturas no equivale a un porcentaje de tiempo.
''')
code('''p5, p95 = train.approxQuantile("humedad_pct", [.05, .95], 0.0)
def describe(frame, timestamp, unit, keys):
    tagged = (frame.withColumn("fecha", F.to_date(timestamp)).withColumn("hora_lima", F.hour(timestamp))
        .withColumn("fuera", ((F.col("humedad_pct") < p5) | (F.col("humedad_pct") > p95)).cast("int")))
    return (tagged.groupBy(*keys).agg(F.count("*").alias(unit), F.avg("humedad_pct").alias("humedad_media_pct"),
        (100*F.avg("fuera")).alias("porcentaje_fuera_referencia")))
silver = spark.read.parquet(str(source / "silver"))
daily = describe(silver, "created_at", "lecturas_validas", ["fecha"])
hourly = describe(silver, "created_at", "lecturas_validas", ["hora_lima"])
minute_summary = describe(data, "minuto", "minutos_observados", ["fecha"])
daily.write.parquet(str(out / "descriptivo_diario_lecturas"))
hourly.write.parquet(str(out / "descriptivo_horario_lecturas"))
minute_summary.write.parquet(str(out / "descriptivo_diario_minutos"))
print("Referencia humedad (%):", {"p5":p5, "p95":p95})
daily.orderBy("fecha").show(40, truncate=False)
hourly.orderBy("hora_lima").show(24, truncate=False)
report = {"run_id": run_id, "s03_run": source.name, "source_sha256": s03["source_sha256"],
    "spark_version": spark.version, "timezone": "America/Lima", "objetivo": "humedad contemporanea media por minuto",
    "split": "cronologico 60/20/20", "conteos": counts, "features": features,
    "validacion": results, "baseline_validacion": baseline_val, "modelo_elegido": winner,
    "test": test_metrics, "baseline_test": baseline_test,
    "supera_baseline_test": test_metrics["rmse"] < baseline_test["rmse"],
    "diferencia_recarga": delta, "p5": p5, "p95": p95,
    "referencia": "primer 60% de minutos observados; no umbral sanitario",
    "fuera_rango_predicciones_test": pred_test.filter(~F.col("prediction").between(0,100)).count()}
(out / "manifest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print("Evidencias:", out)
for frame in [data, train, validation, test, development, pred_test]: frame.unpersist()
''')
md('''## Interpretación para sustentación
- RMSE penaliza más los errores grandes; MAE expresa el error absoluto promedio.
- R² negativo indica peor ajuste que predecir la media del conjunto evaluado. La referencia operativa usa la media del entrenamiento, no la del test.
- El modelo elegido es el mejor ML de la comparación en validación; su utilidad depende de la prueba final y de la comparación con la referencia. Guardarlo no demuestra que sea útil en producción.
- Los huecos y el único sensor limitan la generalización. No se modifican métricas ni se cambia el ganador mirando el test.
- U2 requerirá otro objetivo: humedad de t+1 minuto, pares temporales correctos y comparación con persistencia. Este cuaderno no implementa esa inferencia.
''')
nb.cells = cells
n.validate(nb)
target = Path(__file__).resolve().parents[1] / 'notebooks/04_modelos_humedad.ipynb'
n.write(nb, target)
print(target)
