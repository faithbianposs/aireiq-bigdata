"""Consumidor de streaming AireIQ (Temperatura) · Spark Structured Streaming.

Flujo: eventos (Kafka o archivos) -> validación del contrato v1 -> ventanas de 1 minuto con
watermark -> modelos guardados en batch (sin reentrenar) -> temperatura proyectada a 15/30/60 min,
alerta y error en vivo.

Ejemplo dentro del contenedor (con Kafka):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      wrangler_temperatura/streaming/consumidor_temperatura.py --fuente kafka --servidores kafka:19092
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
    T.StructField("dispositivo",    T.StringType()),
    T.StructField("created_at",     T.StringType()),
    T.StructField("temperatura_c",  T.DoubleType()),
    T.StructField("humedad_pct",    T.DoubleType()),
    T.StructField("presion_hpa",    T.DoubleType()),
    T.StructField("gas_ohms",       T.DoubleType()),
    T.StructField("iaq_estimado",   T.DoubleType()),
    T.StructField("altura_m",       T.DoubleType()),
    T.StructField("calidad_aire",   T.StringType()),
])

SALIDA = T.StructType([
    T.StructField("minuto",            T.TimestampType()),
    T.StructField("temp_real",         T.DoubleType()),
    T.StructField("n_validas",         T.LongType()),
    T.StructField("n_invalidas",       T.LongType()),
    T.StructField("temp_proy_15m",     T.DoubleType()),
    T.StructField("temp_proy_30m",     T.DoubleType()),
    T.StructField("temp_proy_60m",     T.DoubleType()),
    T.StructField("alerta_p95",        T.IntegerType()),
    T.StructField("proyeccion_previa", T.DoubleType()),
    T.StructField("error_modelo",      T.DoubleType()),
    T.StructField("error_persistencia",T.DoubleType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fuente",          choices=["kafka", "archivos"], default="kafka")
    ap.add_argument("--servidores",      default="kafka:19092")
    ap.add_argument("--topico",          default="aireiq-eventos")
    ap.add_argument("--inicio",          choices=["earliest", "latest"], default="earliest")
    ap.add_argument("--carpeta-archivos",default=str(ROOT / "data" / "stream_in"))
    ap.add_argument("--watermark",       default="2 minutes")
    ap.add_argument("--una-vez",         action="store_true")
    ap.add_argument("--checkpoint",      default=str(ROOT / "data" / "checkpoints" / "wrangler_temperatura"))
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("aireiq-consumidor-temperatura")
             .config("spark.sql.session.timeZone", "America/Lima")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Cargar umbrales y modelos guardados en batch (no se reentrena)
    with open(ROOT / "models" / "wrangler_temperatura" / "umbrales.json") as f:
        umbrales = json.load(f)
    with open(ROOT / "models" / "wrangler_temperatura" / "media_por_hora.json") as f:
        media_hora = {int(k): v for k, v in json.load(f).items()}

    p95 = umbrales["p95"]
    p5  = umbrales["p5"]
    modelo_15 = PipelineModel.load(str(ROOT / "models" / "wrangler_temperatura" / "lr_15m"))
    modelo_30 = PipelineModel.load(str(ROOT / "models" / "wrangler_temperatura" / "lr_30m"))
    modelo_60 = PipelineModel.load(str(ROOT / "models" / "wrangler_temperatura" / "lr_60m"))
    print(f"p5={p5:.2f} p95={p95:.2f} | modelos lr_15m, lr_30m, lr_60m cargados")

    # ---- 1. Origen ----
    if a.fuente == "kafka":
        crudo = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", a.servidores)
                 .option("subscribe", a.topico)
                 .option("startingOffsets", a.inicio).load())
        ev = crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e")).select("e.*")
    else:
        ev = (spark.readStream.schema(ESQUEMA)
              .option("maxFilesPerTrigger", 1).json(a.carpeta_archivos))

    # ---- 2. Validación del contrato (gas_ohms=0 es fallo del sensor) ----
    ev = ev.withColumn("ts", F.to_timestamp("created_at")).filter(F.col("ts").isNotNull())
    valida = F.coalesce(
        (F.col("gas_ohms") > 0)
        & F.col("temperatura_c").between(13.0, 35.0)
        & F.col("humedad_pct").between(0, 100)
        & F.col("presion_hpa").between(300, 1100),
        F.lit(False))

    # ---- 3. Ventanas de 1 minuto con watermark ----
    ventanas = (ev.withWatermark("ts", a.watermark)
                .groupBy(F.window("ts", "1 minute").alias("w"))
                .agg(F.avg(F.when(valida, F.col("temperatura_c"))).alias("temperatura_c"),
                     F.avg(F.when(valida, F.col("humedad_pct"))).alias("humedad_pct"),
                     F.avg(F.when(valida, F.col("presion_hpa"))).alias("presion_hpa"),
                     F.sum(valida.cast("int")).alias("n_validas"),
                     F.sum((~valida).cast("int")).alias("n_invalidas"))
                .select(F.col("w.start").alias("minuto"),
                        "temperatura_c", "humedad_pct", "presion_hpa",
                        "n_validas", "n_invalidas"))

    # ---- 4. Estado entre micro-lotes ----
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
            temp = r["temperatura_c"]
            if r["n_validas"] and temp is not None:
                historial[m] = temp
                l15 = historial.get(m - pd.Timedelta(minutes=15))
                l30 = historial.get(m - pd.Timedelta(minutes=30))
                l60 = historial.get(m - pd.Timedelta(minutes=60))
                # Necesita los 3 rezagos para proyectar
                if l15 is not None and l30 is not None and l60 is not None:
                    h = m.hour
                    feats.append(dict(
                        temperatura_c=temp,
                        temp_lag15=l15, temp_lag30=l30, temp_lag60=l60,
                        delta_15=temp - l15,
                        humedad_pct=r["humedad_pct"],
                        presion_hpa=r["presion_hpa"],
                        hora_sin=math.sin(2 * math.pi * h / 24),
                        hora_cos=math.cos(2 * math.pi * h / 24),
                        minuto=r["minuto"]
                    ))

        # Proyecciones con los 3 modelos
        proy_15, proy_30, proy_60 = {}, {}, {}
        if feats:
            df_f = spark.createDataFrame(pd.DataFrame(feats))
            for x in modelo_15.transform(df_f).select("minuto", "prediction").collect():
                proy_15[pd.Timestamp(x["minuto"])] = x["prediction"]
            for x in modelo_30.transform(df_f).select("minuto", "prediction").collect():
                proy_30[pd.Timestamp(x["minuto"])] = x["prediction"]
            for x in modelo_60.transform(df_f).select("minuto", "prediction").collect():
                proy_60[pd.Timestamp(x["minuto"])] = x["prediction"]

        salida = []
        for r in filas:
            m = pd.Timestamp(r["minuto"])
            previa = ep = em = None
            temp = r["temperatura_c"]
            if r["n_validas"] and temp is not None and m in pendientes:
                previa, base = pendientes.pop(m)
                em = temp - previa
                ep = temp - base
                err_m.append(em)
                err_p.append(ep)

            p15 = proy_15.get(m)
            p30 = proy_30.get(m)
            p60 = proy_60.get(m)

            # Guardar proyección de 60m para evaluar cuando llegue ese minuto
            if p60 is not None:
                pendientes[m + pd.Timedelta(minutes=60)] = (p60, temp)

            alerta = None if p60 is None else int(p60 > p95)

            salida.append((
                r["minuto"], temp,
                int(r["n_validas"] or 0), int(r["n_invalidas"] or 0),
                p15, p30, p60, alerta, previa, em, ep
            ))

        spark.createDataFrame(salida, SALIDA).write.mode("append").parquet(
            str(gold / "streaming_temperatura"))

        n = len(err_m)
        rmse = lambda v: (sum(e * e for e in v) / len(v)) ** 0.5
        resumen = dict(
            lote=int(lote_id),
            evaluaciones=n,
            rmse_modelo=      round(rmse(err_m), 3) if n else None,
            rmse_persistencia=round(rmse(err_p), 3) if n else None,
            alertas_p95=int(sum(1 for s in salida if s[7] == 1)),
            lecturas_invalidas=int(sum(s[3] for s in salida)),
        )
        (gold / "streaming_temperatura_metricas.json").write_text(
            json.dumps(resumen, indent=2))
        print(f"[lote {lote_id}] {len(filas)} minutos | {resumen}")

    # ---- 5. Arranque ----
    q = (ventanas.writeStream.outputMode("append").foreachBatch(procesar)
         .option("checkpointLocation", a.checkpoint))
    q = q.trigger(availableNow=True) if a.una_vez else q.trigger(processingTime="5 seconds")
    q.start().awaitTermination()


if __name__ == "__main__":
    main()