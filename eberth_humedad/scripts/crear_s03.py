"""Construye S3. Las salidas se generan exclusivamente al ejecutar Spark."""
from pathlib import Path
import nbformat as n

nb = n.v4.new_notebook()
nb.metadata.kernelspec = dict(display_name='Python 3 (ipykernel)', language='python', name='python3')
cells = []
def md(s): cells.append(n.v4.new_markdown_cell(s))
def code(s): cells.append(n.v4.new_code_cell(s))

md('''# AireIQ · S3: calidad y Parquet de humedad
**Eberth Emmanuel Rosales Castillo — Unidad I.**

Objetivo: transformar el CSV original en datos analíticos trazables. Cada ejecución genera una carpeta nueva; no sobrescribe la fuente ni otras evidencias.

Reglas: campos esenciales presentes y convertibles, números finitos, humedad entre 0 y 100 %, temperatura entre −40 y 85 °C y presión entre 300 y 1100 hPa. Estos son límites amplios de admisibilidad, no rangos de confort ni umbrales aprendidos. Se conserva la presión propia de gran altitud. Se aplica la exclusión `gas_ohms <= 0` del brief a todas las dimensiones como criterio conservador de fallo del sensor; no implica que toda humedad de 100 % sea inválida.

Duplicados: clave `(dispositivo, created_at)`; conservar determinísticamente la fila admisible de menor ID numérico, con desempate por contenido. Las demás se registran en cuarentena. No se imputan mediciones ausentes.

Gold promedia por minuto para reducir el efecto de las dos frecuencias de captura. No rellena minutos faltantes y marca tramos separados por más de 10 minutos. Los futuros pares de pronóstico deberán además estar exactamente a un minuto.

Referencia: https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/
''')
code('''from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, math
from pyspark.sql import SparkSession, functions as F, types as T, Window

spark = (SparkSession.builder.appName("AireIQ-S3-Humedad").master("local[2]")
    .config("spark.sql.session.timeZone", "America/Lima")
    .config("spark.sql.shuffle.partitions", "4").getOrCreate())
source = Path("/opt/aireiq/data/mediciones_aire_rows.csv")
run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
out = Path("/opt/aireiq/outputs/s03") / run
out.mkdir(parents=True, exist_ok=False)
names = ["id", "created_at", "dispositivo", "temperatura_c", "humedad_pct", "presion_hpa", "gas_ohms", "altura_m", "iaq_estimado", "eco2_estimado_ppm", "voc_indice_estimado", "calidad_aire"]
# Bronze conserva texto para poder auditar valores que no se convierten.
schema = T.StructType([T.StructField(c, T.StringType(), True) for c in names])
raw = (spark.read.option("header", True).option("mode", "FAILFAST")
       .option("enforceSchema", False).schema(schema).csv(str(source))).cache()
total = raw.count()
assert total > 0
raw.write.mode("errorifexists").parquet(str(out / "bronze"))
print("Lecturas de entrada:", total)
''')
code('''numeric = names[3:11]
def validar(frame):
    d = frame.withColumn("raw_json", F.to_json(F.struct(*[F.col(c) for c in names])))
    for c in names:
        d = d.withColumn(c, F.when(F.length(F.trim(F.col(c))) == 0, F.lit(None)).otherwise(F.trim(F.col(c))))
    for c in numeric:
        d = d.withColumn(c, F.expr(f"try_cast(`{c}` as double)"))
    d = d.withColumn("created_at", F.expr("try_cast(created_at as timestamp)"))
    required = ["id", "created_at", "dispositivo", "temperatura_c", "humedad_pct", "presion_hpa", "gas_ohms"]
    reasons = [F.when(F.col(c).isNull(), F.lit("ausente_o_tipo_invalido:" + c)) for c in required]
    for c in numeric:
        reasons.append(F.when(F.isnan(c) | (F.abs(F.col(c)) == float("inf")), F.lit("no_finito:" + c)))
    for c, lo, hi in [("humedad_pct", 0, 100), ("temperatura_c", -40, 85), ("presion_hpa", 300, 1100)]:
        reasons.append(F.when(~F.col(c).between(lo, hi), F.lit("fuera_rango:" + c)))
    reasons.append(F.when(F.col("gas_ohms") <= 0, F.lit("fallo_sensor_gas")))
    return d.withColumn("motivos", F.filter(F.array(*reasons), lambda x: x.isNotNull()))

checked = validar(raw).cache()
invalid = checked.filter(F.size("motivos") > 0)
eligible = checked.filter(F.size("motivos") == 0)
order = Window.partitionBy("dispositivo", "created_at").orderBy(
    F.expr("try_cast(id as bigint)").asc_nulls_last(), F.col("id"), F.col("raw_json"))
ranked = eligible.withColumn("orden", F.row_number().over(order))
duplicates = ranked.filter("orden > 1").drop("orden").withColumn("motivos", F.array(F.lit("duplicado_dispositivo_fecha")))
quarantine = invalid.unionByName(duplicates).cache()
silver = (ranked.filter("orden = 1").drop("orden", "motivos", "raw_json")
    .withColumn("fecha", F.to_date("created_at"))).cache()
valid_count, rejected_count = silver.count(), quarantine.count()
assert total == valid_count + rejected_count
assert silver.select("dispositivo", "created_at").distinct().count() == valid_count
print({"entrada": total, "silver": valid_count, "cuarentena": rejected_count, "duplicados_admisibles": duplicates.count()})
quarantine.select(F.explode("motivos").alias("motivo")).groupBy("motivo").count().show(truncate=False)
''')
md('''## Pruebas de las reglas
Casos sintéticos separados del dataset: valor vacío, dato no numérico, NaN, infinito, fallo de gas y extremos de humedad. Estas filas nunca se mezclan con las mediciones reales.''')
code('''base = raw.first().asDict()
cases = [("valida", {}, True), ("vacio", {"humedad_pct":" "}, False),
         ("texto", {"humedad_pct":"abc"}, False), ("nan", {"humedad_pct":"NaN"}, False),
         ("infinito", {"gas_ohms":"Infinity"}, False), ("gas_cero", {"gas_ohms":"0"}, False),
         ("humedad_alta", {"humedad_pct":"101"}, False),
         ("fecha_mala", {"created_at":"fecha-invalida"}, False),
         ("humedad_100", {"humedad_pct":"100", "gas_ohms":"1000"}, True)]
rows = [{**base, **changes, "id": label} for label, changes, _ in cases]
results = {r.id: len(r.motivos) == 0 for r in validar(spark.createDataFrame(rows, schema)).select("id", "motivos").collect()}
assert results == {label: expected for label, _, expected in cases}, results
print("Pruebas de calidad superadas:", len(cases))
''')
code('''minute = (silver.groupBy("dispositivo", F.window("created_at", "1 minute").alias("ventana"))
    .agg(F.avg("humedad_pct").alias("humedad_pct"),
         F.avg("temperatura_c").alias("temperatura_c"),
         F.avg("presion_hpa").alias("presion_hpa"), F.count("*").alias("lecturas"))
    .withColumn("minuto", F.col("ventana.start")).drop("ventana"))
w = Window.partitionBy("dispositivo").orderBy("minuto")
gold = (minute.withColumn("anterior", F.lag("minuto").over(w))
    .withColumn("salto_segundos", F.col("minuto").cast("long") - F.col("anterior").cast("long"))
    .withColumn("nuevo_tramo", F.when(F.col("anterior").isNull() | (F.col("salto_segundos") > 600), 1).otherwise(0))
    .withColumn("tramo", F.sum("nuevo_tramo").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("fecha", F.to_date("minuto")).withColumn("hora_lima", F.hour("minuto"))
    .drop("anterior", "nuevo_tramo")).cache()
assert gold.agg(F.sum("lecturas")).first()[0] == valid_count
silver.write.mode("errorifexists").partitionBy("fecha").parquet(str(out / "silver"))
gold.write.mode("errorifexists").partitionBy("fecha").parquet(str(out / "gold"))
quarantine.write.mode("errorifexists").parquet(str(out / "cuarentena"))
gold.orderBy("minuto").show(10, truncate=False)
''')
code('''read_silver = spark.read.parquet(str(out / "silver"))
read_gold = spark.read.parquet(str(out / "gold"))
assert read_silver.count() == valid_count
assert read_gold.count() == gold.count()
# Comparación multiconjunto: verifica valores, no solo los conteos.
for expected, actual in [(silver, read_silver), (gold, read_gold)]:
    a, b = expected.select(sorted(expected.columns)), actual.select(sorted(expected.columns))
    assert a.exceptAll(b).limit(1).count() == 0
    assert b.exceptAll(a).limit(1).count() == 0
date_value = read_silver.agg(F.min("fecha")).first()[0]
filtered = read_silver.filter(F.col("fecha") == F.lit(date_value))
filtered.explain(True)
plan = filtered._jdf.queryExecution().executedPlan().toString()
assert "PartitionFilters:" in plan and "PartitionFilters: []" not in plan
assert filtered.count() == silver.filter(F.col("fecha") == F.lit(date_value)).count()
(out / "plan_partition_filters.txt").write_text(plan, encoding="utf-8")
report = {"run_id": run, "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
          "spark_version": spark.version, "master": spark.sparkContext.master,
          "timezone": "America/Lima", "entrada": total, "silver": valid_count,
          "cuarentena": rejected_count, "gold_minutos": gold.count(),
          "tramos": gold.select("dispositivo", "tramo").distinct().count(),
          "pruebas_sinteticas": len(cases), "roundtrip_valores": True,
          "partition_filters": True,
          "motivos": {r.motivo:r['count'] for r in quarantine.select(F.explode("motivos").alias("motivo")).groupBy("motivo").count().collect()}}
(out / "manifest.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
print(json.dumps(report, indent=2, ensure_ascii=False))
print("Evidencias:", out)
raw.unpersist(); checked.unpersist(); silver.unpersist(); quarantine.unpersist(); gold.unpersist()
''')
md('''## Lectura del resultado y límites
Silver conserva lecturas válidas; Gold tiene una fila por minuto observado. Los promedios por minuto no representan intervalos sin observaciones. La cuarentena conserva razones y contenido original para auditoría. Las frecuencias de motivos pueden solaparse.

S3 no entrena modelos ni fija todavía p5/p95. S4 calculará referencias y transformaciones solo con el periodo de entrenamiento para evitar fuga de información. Este entorno ejecuta Spark en dos hilos locales; no demuestra un despliegue multinodo.
''')
nb.cells = cells
n.validate(nb)
target = Path(__file__).resolve().parents[1] / 'notebooks/03_calidad_humedad.ipynb'
n.write(nb, target)
print(target)
