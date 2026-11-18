"""Productor AireIQ: reproduce el CSV histórico como eventos del contrato v1.

Es una SIMULACIÓN declarada (el sensor real publica a Supabase). Destinos:
  kafka     -> topic de Kafka (key = dispositivo)
  archivos  -> archivos JSON en data/stream_in/ (para probar sin Kafka)

Ejemplo dentro del contenedor:
  python frank_iaq/streaming/productor_csv.py --destino kafka --servidores kafka:19092
"""
import argparse
import json
import math
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def evento(r):
    def num(x):
        return None if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)
    return {
        "schema_version": 1,
        "dispositivo": r["dispositivo"],
        "created_at": r["created_at"].isoformat(),
        "temperatura_c": num(r["temperatura_c"]),
        "humedad_pct": num(r["humedad_pct"]),
        "presion_hpa": num(r["presion_hpa"]),
        "gas_ohms": num(r["gas_ohms"]),
        "iaq_estimado": num(r["iaq_estimado"]),
        "altura_m": num(r["altura_m"]),
        "calidad_aire": None if pd.isna(r["calidad_aire"]) else str(r["calidad_aire"]),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(ROOT / "data" / "raw" / "mediciones_aire.csv"))
    ap.add_argument("--destino", choices=["kafka", "archivos"], default="kafka")
    ap.add_argument("--servidores", default="kafka:19092")
    ap.add_argument("--topico", default="aireiq-eventos")
    ap.add_argument("--carpeta-salida", default=str(ROOT / "data" / "stream_in"))
    ap.add_argument("--pausa", type=float, default=0.02, help="segundos entre eventos (kafka)")
    ap.add_argument("--lote", type=int, default=250, help="eventos por archivo (archivos)")
    ap.add_argument("--desde", default=None, help="fecha mínima, p. ej. 2026-11-05")
    ap.add_argument("--limite", type=int, default=None)
    a = ap.parse_args()

    df = pd.read_csv(a.csv)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, format="mixed").dt.tz_convert("America/Lima")
    df = df.sort_values("created_at")
    if a.desde:
        df = df[df["created_at"] >= pd.Timestamp(a.desde, tz="America/Lima")]
    if a.limite:
        df = df.head(a.limite)
    eventos = [evento(r) for _, r in df.iterrows()]
    print(f"{len(eventos)} eventos listos ({eventos[0]['created_at']} -> {eventos[-1]['created_at']})")

    if a.destino == "kafka":
        from kafka import KafkaProducer
        prod = KafkaProducer(bootstrap_servers=a.servidores,
                             key_serializer=lambda k: k.encode(),
                             value_serializer=lambda v: json.dumps(v).encode())
        for i, e in enumerate(eventos, 1):
            prod.send(a.topico, key=e["dispositivo"], value=e)
            if a.pausa:
                time.sleep(a.pausa)
            if i % 500 == 0:
                print(f"  enviados {i}")
        prod.flush()
        print("Listo: eventos enviados a", a.topico)
    else:
        salida = Path(a.carpeta_salida)
        salida.mkdir(parents=True, exist_ok=True)
        for n, i in enumerate(range(0, len(eventos), a.lote)):
            lote = eventos[i:i + a.lote]
            (salida / f"eventos_{n:05d}.json").write_text("\n".join(json.dumps(e) for e in lote))
        print("Listo: archivos en", salida)


if __name__ == "__main__":
    main()
