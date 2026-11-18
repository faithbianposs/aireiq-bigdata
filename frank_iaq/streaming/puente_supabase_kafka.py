"""Puente Supabase -> Kafka (contrato v1 de aireiq-eventos).

Lee las filas nuevas de public.mediciones_aire con un usuario de SOLO LECTURA y las publica en
Kafka. Guarda el último id enviado en data/puente_estado.json para no repetir ni perder filas.

Variables de entorno (en tu .env, nunca en Git):
  SUPABASE_DB_URL   postgresql://aireiq_lector:CLAVE@HOST:5432/postgres

Dentro del contenedor:
  python frank_iaq/streaming/puente_supabase_kafka.py --servidores kafka:19092
"""
import argparse
import json
import math
import os
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
ESTADO = ROOT / "data" / "puente_estado.json"
SQL = ("select id, created_at, dispositivo, temperatura_c, humedad_pct, presion_hpa, gas_ohms, "
       "altura_m, iaq_estimado, calidad_aire from public.mediciones_aire "
       "where id > %s order by id limit %s")


def num(x):
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)


def a_evento(f):
    _id, ts, disp, t, h, p, g, alt, iaq, cal = f
    return {"schema_version": 1, "dispositivo": disp,
            "created_at": pd.Timestamp(ts).tz_convert("America/Lima").isoformat(),
            "temperatura_c": num(t), "humedad_pct": num(h), "presion_hpa": num(p),
            "gas_ohms": num(g), "iaq_estimado": num(iaq), "altura_m": num(alt),
            "calidad_aire": cal}


def leer_estado(desde_id):
    if desde_id is not None:
        return desde_id
    if ESTADO.exists():
        return json.loads(ESTADO.read_text())["ultimo_id"]
    csv = ROOT / "data" / "raw" / "mediciones_aire.csv"
    return int(pd.read_csv(csv, usecols=["id"])["id"].max())   # lo que ya se reprodujo del CSV


def un_ciclo(conn, publicar, ultimo_id, limite=500):
    cur = conn.cursor()
    cur.execute(SQL, (ultimo_id, limite))
    filas = cur.fetchall()
    for f in filas:
        publicar(a_evento(f))
        ultimo_id = f[0]
    return ultimo_id, len(filas)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--servidores", default="kafka:19092")
    ap.add_argument("--topico", default="aireiq-eventos")
    ap.add_argument("--cada", type=float, default=10, help="segundos entre consultas")
    ap.add_argument("--desde-id", type=int, default=None, help="forzar el id inicial")
    ap.add_argument("--una-vez", action="store_true")
    a = ap.parse_args()

    import psycopg2
    from kafka import KafkaProducer
    url = os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("Falta SUPABASE_DB_URL en el .env")
    prod = KafkaProducer(bootstrap_servers=a.servidores, key_serializer=lambda k: k.encode(),
                         value_serializer=lambda v: json.dumps(v).encode())
    publicar = lambda e: prod.send(a.topico, key=e["dispositivo"], value=e)

    ultimo = leer_estado(a.desde_id)
    print(f"Puente activo. Último id enviado: {ultimo}")
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(url)
                conn.autocommit = True
            ultimo, n = un_ciclo(conn, publicar, ultimo)
            if n:
                prod.flush()
                ESTADO.parent.mkdir(parents=True, exist_ok=True)
                ESTADO.write_text(json.dumps({"ultimo_id": ultimo}))
                print(f"{time.strftime('%H:%M:%S')} enviadas {n} filas nuevas (último id {ultimo})")
        except Exception as e:              # red caída, pausa de Supabase, etc.
            print("Aviso:", repr(e)[:200], "- reintento")
            conn = None
        if a.una_vez:
            break
        time.sleep(a.cada)


if __name__ == "__main__":
    main()
