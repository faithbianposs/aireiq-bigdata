"""Consumidor de streaming AireIQ (presión barométrica) · Spark Structured Streaming. Autor: Marco.

Flujo: eventos (Kafka o archivos) -> validación del contrato v1 -> ventanas de 1 minuto con watermark
-> modelo guardado en batch (sin reentrenar) -> presión proyectada a H minutos, alerta de caída fuerte
y error en vivo frente a la persistencia.

Usa el mismo tópico que el resto del equipo (aireiq-eventos): cada evento ya trae presion_hpa.

Ejemplos dentro del contenedor:
  # sin Kafka, con archivos (primero: python frank_iaq/streaming/productor_csv.py --destino archivos)
  spark-submit marco_presion/streaming/consumidor_presion.py --fuente archivos --una-vez
  # con Kafka
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      marco_presion/streaming/consumidor_presion.py --fuente kafka --servidores kafka:19092
"""
import argparse
import json
import math
from pathlib import Path

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[2]
DOS_PI = 2 * math.pi

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
    T.StructField("presion_real", T.DoubleType()),
    T.StructField("n_validas", T.LongType()),
    T.StructField("n_invalidas", T.LongType()),
    T.StructField("presion_proyectada", T.DoubleType()),  # proyección hecha en este minuto para minuto + H
    T.StructField("cambio_previsto", T.DoubleType()),     # presion_proyectada - presion_real
    T.StructField("alerta", T.IntegerType()),             # 1 si el cambio previsto llega al límite de aviso
    T.StructField("proyeccion_previa", T.DoubleType()),   # lo que se proyectó H minutos atrás para este minuto
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
    ap.add_argument("--horizonte", type=int, default=180, choices=[60, 180])
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--una-vez", action="store_true", help="procesa lo disponible y termina")
    ap.add_argument("--checkpoint", default=str(ROOT / "data" / "checkpoints" / "consumidor_presion"))
    a = ap.parse_args()
    H = a.horizonte
    modelos = ROOT / "models" / "marco_presion"

    spark = (SparkSession.builder.appName("aireiq-consumidor-presion")
             .config("spark.sql.session.timeZone", "America/Lima")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    modelo = PipelineModel.load(str(modelos / f"presion_h{H}"))
    limite = None
    if H == 180 and (modelos / "alerta_caida_3h.json").exists():
        limite = json.loads((modelos / "alerta_caida_3h.json").read_text())["limite_aviso_hpa"]
    print(f"modelo presion_h{H} cargado (sin reentrenar) | límite de aviso: {limite} hPa")

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

    # ---- 2. Validación del contrato (mismas reglas que el equipo; gas_ohms = 0 es fallo del sensor) ----
    ev = ev.withColumn("ts", F.to_timestamp("created_at")).filter(F.col("ts").isNotNull())
    valida = F.coalesce(
        (F.col("gas_ohms") > 0)
        & F.col("temperatura_c").between(-40, 85)
        & F.col("humedad_pct").between(0, 100)
        & F.col("presion_hpa").between(300, 1100)
        & F.col("iaq_estimado").between(0, 500), F.lit(False))

    # ---- 3. Ventanas de 1 minuto con watermark ----
    # La hora del día se calcula aquí, en Spark, con la zona de Lima: así coincide con el entrenamiento.
    ventanas = (ev.withWatermark("ts", a.watermark)
                .groupBy(F.window("ts", "1 minute").alias("w"))
                .agg(F.avg(F.when(valida, F.col("presion_hpa"))).alias("presion_hpa"),
                     F.sum(valida.cast("int")).alias("n_validas"),
                     F.sum((~valida).cast("int")).alias("n_invalidas"))
                .select(F.col("w.start").alias("minuto"), "presion_hpa", "n_validas", "n_invalidas",
                        (F.hour("w.start") + F.minute("w.start") / 60.0).alias("hora_dec")))

    # ---- 4. Estado entre micro-lotes (historial para el rezago de 60 min y los errores en vivo) ----
    historial, pendientes = {}, {}
    err_m, err_p = [], []
    gold = ROOT / "data" / "gold"

    def procesar(lote, lote_id):
        filas = lote.orderBy("minuto").collect()
        if not filas:
            return
        feats = []
        for i, r in enumerate(filas):
            m = pd.Timestamp(r["minuto"])
            if r["n_validas"] and r["presion_hpa"] is not None:
                historial[m] = r["presion_hpa"]
                lag60 = historial.get(m - pd.Timedelta(minutes=60))
                if lag60 is not None:
                    h = r["hora_dec"]
                    feats.append(dict(clave=i, presion_hpa=r["presion_hpa"], presion_lag60=lag60,
                                      presion_d60=r["presion_hpa"] - lag60,
                                      h24_sin=math.sin(DOS_PI * h / 24), h24_cos=math.cos(DOS_PI * h / 24),
                                      h12_sin=math.sin(DOS_PI * h / 12), h12_cos=math.cos(DOS_PI * h / 12)))
        proy = {}
        if feats:
            df = spark.createDataFrame(pd.DataFrame(feats))
            for x in modelo.transform(df).select("clave", "prediction").collect():
                proy[int(x["clave"])] = float(x["prediction"])

        salida = []
        for i, r in enumerate(filas):
            m = pd.Timestamp(r["minuto"])
            real = r["presion_hpa"] if r["n_validas"] else None
            previa = ep = em = None
            if real is not None and m in pendientes:
                previa, base = pendientes.pop(m)
                em, ep = real - previa, real - base
                err_m.append(em)
                err_p.append(ep)
            p = proy.get(i)
            cambio = alerta = None
            if p is not None:
                pendientes[m + pd.Timedelta(minutes=H)] = (p, real)
                cambio = p - real
                alerta = None if limite is None else int(cambio <= limite)
            salida.append((r["minuto"], real, int(r["n_validas"] or 0), int(r["n_invalidas"] or 0),
                           p, cambio, alerta, previa, em, ep))
        spark.createDataFrame(salida, SALIDA).write.mode("append").parquet(str(gold / "streaming_presion"))

        n = len(err_m)
        rmse = lambda v: (sum(e * e for e in v) / len(v)) ** 0.5
        resumen = dict(lote=int(lote_id), horizonte_min=H, evaluaciones=n,
                       rmse_modelo=round(rmse(err_m), 3) if n else None,
                       rmse_persistencia=round(rmse(err_p), 3) if n else None,
                       alertas=int(sum(1 for s in salida if s[6] == 1)),
                       lecturas_invalidas=int(sum(s[3] for s in salida)))
        (gold / "streaming_presion_metricas.json").write_text(json.dumps(resumen, indent=2))
        print(f"[lote {lote_id}] {len(filas)} minutos | {resumen}")

    # ---- 5. Arranque ----
    q = (ventanas.writeStream.outputMode("append").foreachBatch(procesar)
         .option("checkpointLocation", a.checkpoint))
    q = q.trigger(availableNow=True) if a.una_vez else q.trigger(processingTime="5 seconds")
    q.start().awaitTermination()


if __name__ == "__main__":
    main()
