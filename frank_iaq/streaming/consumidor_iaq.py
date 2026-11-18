"""Consumidor de streaming AireIQ (IAQ) · Spark Structured Streaming.

Flujo: eventos (Kafka o archivos) -> validación del contrato v1 -> ventanas de 1 minuto con
watermark -> modelo guardado en batch (sin reentrenar) -> IAQ proyectado, alerta y error en vivo.

Ejemplo dentro del contenedor (con Kafka):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      frank_iaq/streaming/consumidor_iaq.py --fuente kafka --servidores kafka:19092
"""
import argparse
import json
import math
from pathlib import Path

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[2]

ESQUEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("dispositivo", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("temperatura_c", T.DoubleType()),
    T.StructField("humedad_pct", T.DoubleType()),
    T.StructField("presion_hpa", T.DoubleType()),
    T.StructField("gas_ohms", T.DoubleType()),
    T.StructField("iaq_estimado", T.DoubleType()),
    T.StructField("altura_m", T.DoubleType()),
    T.StructField("calidad_aire", T.StringType()),
])

SALIDA = T.StructType([
    T.StructField("minuto", T.TimestampType()),
    T.StructField("iaq_real", T.DoubleType()),
    T.StructField("n_validas", T.LongType()),
    T.StructField("n_invalidas", T.LongType()),
    T.StructField("iaq_proyectado", T.DoubleType()),     # proyección hecha en este minuto para minuto + H
    T.StructField("alerta", T.IntegerType()),            # 1 si la proyección supera el p95
    T.StructField("proyeccion_previa", T.DoubleType()),  # lo que se proyectó H minutos atrás para este minuto
    T.StructField("error_modelo", T.DoubleType()),
    T.StructField("error_persistencia", T.DoubleType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fuente", choices=["kafka", "archivos"], default="kafka")
    ap.add_argument("--servidores", default="kafka:19092")
    ap.add_argument("--topico", default="aireiq-eventos")
    ap.add_argument("--inicio", choices=["earliest", "latest"], default="earliest")
    ap.add_argument("--carpeta-archivos", default=str(ROOT / "data" / "stream_in"))
    ap.add_argument("--horizonte", type=int, default=10, choices=[5, 10, 15])
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--una-vez", action="store_true", help="procesa lo disponible y termina")
    ap.add_argument("--checkpoint", default=str(ROOT / "data" / "checkpoints" / "consumidor_iaq"))
    a = ap.parse_args()
    H = a.horizonte

    spark = (SparkSession.builder.appName("aireiq-consumidor-iaq")
             .config("spark.sql.session.timeZone", "America/Lima")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    p95 = spark.read.parquet(str(ROOT / "data" / "gold" / "umbral_iaq")).first()["p95"]
    modelo = PipelineModel.load(str(ROOT / "models" / "frank_iaq" / f"iaq_h{H}"))
    print(f"p95 = {p95:.2f} | modelo iaq_h{H} cargado (sin reentrenar)")

    # ---- 1. Origen ----
    if a.fuente == "kafka":
        crudo = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", a.servidores)
                 .option("subscribe", a.topico)
                 .option("startingOffsets", a.inicio).load())
        ev = crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e")).select("e.*")
    else:
        ev = (spark.readStream.schema(ESQUEMA).option("maxFilesPerTrigger", 1)
              .json(a.carpeta_archivos))

    # ---- 2. Validación del contrato (rangos físicos; gas_ohms = 0 es fallo del sensor) ----
    ev = ev.withColumn("ts", F.to_timestamp("created_at")).filter(F.col("ts").isNotNull())
    valida = F.coalesce(
        (F.col("gas_ohms") > 0)
        & F.col("temperatura_c").between(-40, 85)
        & F.col("humedad_pct").between(0, 100)
        & F.col("presion_hpa").between(300, 1100)
        & F.col("iaq_estimado").between(0, 500), F.lit(False))

    # ---- 3. Ventanas de 1 minuto con watermark ----
    ventanas = (ev.withWatermark("ts", a.watermark)
                .groupBy(F.window("ts", "1 minute").alias("w"))
                .agg(F.avg(F.when(valida, F.col("iaq_estimado"))).alias("iaq"),
                     F.avg(F.when(valida, F.col("temperatura_c"))).alias("temperatura_c"),
                     F.avg(F.when(valida, F.col("humedad_pct"))).alias("humedad_pct"),
                     F.avg(F.when(valida, F.col("presion_hpa"))).alias("presion_hpa"),
                     F.sum(valida.cast("int")).alias("n_validas"),
                     F.sum((~valida).cast("int")).alias("n_invalidas"))
                .select(F.col("w.start").alias("minuto"), "iaq", "temperatura_c", "humedad_pct",
                        "presion_hpa", "n_validas", "n_invalidas"))

    # ---- 4. Estado entre micro-lotes (historial para los rezagos y errores en vivo) ----
    historial, pendientes = {}, {}
    err_m, err_p = [], []
    gold = ROOT / "data" / "gold"

    def procesar(lote, lote_id):
        filas = lote.orderBy("minuto").collect()
        if not filas:
            return
        feats, claves = [], []
        for r in filas:
            m = pd.Timestamp(r["minuto"])
            if r["n_validas"] and r["iaq"] is not None:
                historial[m] = r["iaq"]
                l5, l10 = historial.get(m - pd.Timedelta(minutes=5)), historial.get(m - pd.Timedelta(minutes=10))
                if l5 is not None and l10 is not None:
                    h = m.hour
                    feats.append(dict(iaq=r["iaq"], iaq_lag5=l5, iaq_lag10=l10, iaq_d5=r["iaq"] - l5,
                                      temperatura_c=r["temperatura_c"], humedad_pct=r["humedad_pct"],
                                      presion_hpa=r["presion_hpa"],
                                      hora_sin=math.sin(2 * math.pi * h / 24),
                                      hora_cos=math.cos(2 * math.pi * h / 24), minuto=r["minuto"]))
        proy = {}
        if feats:
            df = spark.createDataFrame(pd.DataFrame(feats))
            for x in modelo.transform(df).select("minuto", "prediction").collect():
                proy[pd.Timestamp(x["minuto"])] = x["prediction"]

        salida = []
        for r in filas:
            m = pd.Timestamp(r["minuto"])
            previa = ep = em = None
            if r["n_validas"] and r["iaq"] is not None and m in pendientes:
                previa, base = pendientes.pop(m)
                em, ep = r["iaq"] - previa, r["iaq"] - base
                err_m.append(em)
                err_p.append(ep)
            p = proy.get(m)
            if p is not None:
                pendientes[m + pd.Timedelta(minutes=H)] = (p, r["iaq"])
            salida.append((r["minuto"], r["iaq"], int(r["n_validas"] or 0), int(r["n_invalidas"] or 0),
                           p, None if p is None else int(p > p95), previa, em, ep))
        spark.createDataFrame(salida, SALIDA).write.mode("append").parquet(str(gold / "streaming_iaq"))

        n = len(err_m)
        rmse = lambda v: (sum(e * e for e in v) / len(v)) ** 0.5
        resumen = dict(lote=int(lote_id), horizonte_min=H, evaluaciones=n,
                       rmse_modelo=round(rmse(err_m), 3) if n else None,
                       rmse_persistencia=round(rmse(err_p), 3) if n else None,
                       alertas=int(sum(1 for s in salida if s[5] == 1)),
                       lecturas_invalidas=int(sum(s[3] for s in salida)))
        (gold / "streaming_iaq_metricas.json").write_text(json.dumps(resumen, indent=2))
        print(f"[lote {lote_id}] {len(filas)} minutos | {resumen}")

    # ---- 5. Arranque ----
    q = (ventanas.writeStream.outputMode("append").foreachBatch(procesar)
         .option("checkpointLocation", a.checkpoint))
    q = q.trigger(availableNow=True) if a.una_vez else q.trigger(processingTime="5 seconds")
    q.start().awaitTermination()


if __name__ == "__main__":
    main()
