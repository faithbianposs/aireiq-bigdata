"""Genera el cuaderno inicial sin ejecutar ni inventar resultados."""
from pathlib import Path
import nbformat as nbf

root = Path(__file__).resolve().parents[1]
nb = nbf.v4.new_notebook()
nb.metadata.kernelspec = {'display_name': 'Python 3 (ipykernel)', 'language': 'python', 'name': 'python3'}
nb.cells = [
    nbf.v4.new_markdown_cell('''# AireIQ · S2: entorno y lectura de humedad
**Responsable:** Eberth Emmanuel Rosales Castillo.

**Descriptiva:** ¿Cómo varía la humedad según la hora y el día y qué porcentaje de lecturas válidas queda fuera del rango histórico p5–p95?

**Predictiva U1:** ¿Qué tan precisamente se estima la humedad con temperatura, presión y hora?

**Predictiva U2:** ¿Cuál será la humedad dentro de un minuto y saldrá del rango histórico?

Este cuaderno comprueba el entorno y la fuente. S3 añadirá calidad completa y Parquet; S4, modelos. El rango histórico no es un umbral sanitario. Arquitectura Lambda: Supabase alimentará batch y streaming. El CSV original se monta como solo lectura.

Referencia de entorno: https://github.com/262bigdata/lambda26/tree/main/pyspark
'''),
    nbf.v4.new_code_cell('''import sys
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName("AireIQ-S2-Humedad")
         .master("local[2]")
         .config("spark.sql.session.timeZone", "America/Lima")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
print("Python:", sys.version)
print("PySpark:", pyspark.__version__)
print("Master:", spark.sparkContext.master)
print("Modo de laboratorio: dos hilos locales; no es un cluster multinodo.")
'''),
    nbf.v4.new_code_cell('''path = Path("/opt/aireiq/data/mediciones_aire_rows.csv")
assert path.is_file(), "Falta el CSV montado por Compose"
schema = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("dispositivo", T.StringType()),
    *[T.StructField(c, T.DoubleType()) for c in [
        "temperatura_c", "humedad_pct", "presion_hpa", "gas_ohms", "altura_m",
        "iaq_estimado", "eco2_estimado_ppm", "voc_indice_estimado"]],
    T.StructField("calidad_aire", T.StringType())
])
df = (spark.read.option("header", True).option("mode", "FAILFAST")
      .option("enforceSchema", False).schema(schema).csv(str(path)))
df.printSchema()
df.select("created_at", "dispositivo", "humedad_pct", "temperatura_c", "presion_hpa").show(5, truncate=False)
'''),
    nbf.v4.new_markdown_cell('''## Transformación y evaluación perezosa
Definir columnas y filtros construye un plan. `explain()` permite inspeccionarlo; `count()` y `show()` solicitan resultados. El filtro siguiente aplica la regla inicial del brief, no sustituye la validación completa de S3. No se eliminan registros del CSV.'''),
    nbf.v4.new_code_cell('''validas = df.filter((F.col("gas_ohms") > 0) & F.col("humedad_pct").between(0, 100))
por_hora = (validas.withColumn("hora_lima", F.hour("created_at"))
            .groupBy("hora_lima")
            .agg(F.count("*").alias("lecturas"), F.avg("humedad_pct").alias("humedad_promedio_pct")))
por_hora.explain(True)
total = df.count()
conservadas = validas.count()
print({"total": total, "conservadas_regla_inicial": conservadas, "excluidas": total-conservadas})
assert total > 0 and 0 < conservadas <= total
assert df.filter(F.col("created_at").isNull()).count() == 0, "Hay fechas no interpretadas"
por_hora.orderBy("hora_lima").show(24)
'''),
    nbf.v4.new_code_cell('''# Ejemplo RDD requerido en S2: conteo por dispositivo.
conteos_rdd = (validas.select("dispositivo").rdd
               .map(lambda row: (row.dispositivo, 1))
               .reduceByKey(lambda a, b: a + b).collect())
print(conteos_rdd)
assert sum(n for _, n in conteos_rdd) == conservadas
'''),
    nbf.v4.new_markdown_cell('''## Evidencia
Ejecutar todas las celdas y guardar el cuaderno ejecutado. No se han precargado salidas.
El promedio mostrado pondera lecturas, no minutos; S3 tratará el cambio de frecuencia de captura.
Siguiente etapa: nulos, duplicados, rangos, cuarentena y Parquet particionado por fecha, con lectura de vuelta.
''')
]
nbf.validate(nb)
destination = root / 'notebooks' / '01_entorno_humedad.ipynb'
destination.parent.mkdir(exist_ok=True)
nbf.write(nb, destination)
print(destination)
