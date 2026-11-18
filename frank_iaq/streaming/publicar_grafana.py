"""Publica las métricas del streaming de IAQ a Pushgateway (S11).

Lee data/gold/streaming_iaq (salida del consumidor) y envía, minuto a minuto, el IAQ real,
la proyección que se hizo 10 minutos antes, la persistencia, el umbral p95, la alerta y los
RMSE acumulados. Prometheus recoge esos valores de Pushgateway y Grafana los dibuja.

Dentro del contenedor:
  python frank_iaq/streaming/publicar_grafana.py --pausa 0.5      # reproduce el tramo en vivo
  python frank_iaq/streaming/publicar_grafana.py --resumen        # solo los valores finales
"""
import argparse
import math
import time
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def texto_metricas(m):
    lineas = []
    for nombre, (ayuda, valor) in m.items():
        if valor is None or (isinstance(valor, float) and math.isnan(valor)):
            continue
        lineas.append(f"# HELP {nombre} {ayuda}")
        lineas.append(f"# TYPE {nombre} gauge")
        lineas.append(f"{nombre} {valor}")
    return "\n".join(lineas) + "\n"


def enviar(url, job, variable, cuerpo):
    req = urllib.request.Request(f"{url}/metrics/job/{job}/variable/{variable}",
                                 data=cuerpo.encode(), method="PUT",
                                 headers={"Content-Type": "text/plain; version=0.0.4"})
    urllib.request.urlopen(req, timeout=10).read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://pushgateway:9091")
    ap.add_argument("--entrada", default=str(ROOT / "data" / "gold" / "streaming_iaq"))
    ap.add_argument("--umbral", default=str(ROOT / "data" / "gold" / "umbral_iaq"))
    ap.add_argument("--pausa", type=float, default=0.5, help="segundos entre minutos")
    ap.add_argument("--resumen", action="store_true", help="enviar solo los valores finales")
    ap.add_argument("--seco", action="store_true", help="no enviar, solo imprimir el último envío")
    a = ap.parse_args()

    df = pd.read_parquet(a.entrada).sort_values("minuto").reset_index(drop=True)
    p95 = float(pd.read_parquet(a.umbral)["p95"].iloc[0])
    print(f"{len(df)} minutos | p95 = {p95:.2f}")

    sq_m = sq_p = 0.0
    n_ev = 0
    validas = invalidas = 0
    ultimo = None
    for i, r in df.iterrows():
        validas += int(r["n_validas"])
        invalidas += int(r["n_invalidas"])
        if pd.notna(r["error_modelo"]):
            sq_m += float(r["error_modelo"]) ** 2
            sq_p += float(r["error_persistencia"]) ** 2
            n_ev += 1
        persist = (float(r["iaq_real"]) - float(r["error_persistencia"])
                   if pd.notna(r["error_persistencia"]) and pd.notna(r["iaq_real"]) else None)
        m = {
            "aireiq_iaq_real": ("IAQ medido en el minuto", None if pd.isna(r["iaq_real"]) else float(r["iaq_real"])),
            "aireiq_iaq_proyectado": ("IAQ proyectado 10 min antes para este minuto", None if pd.isna(r["proyeccion_previa"]) else float(r["proyeccion_previa"])),
            "aireiq_iaq_persistencia": ("IAQ de hace 10 min (persistencia)", persist),
            "aireiq_iaq_umbral_p95": ("Umbral p95 del periodo de referencia", p95),
            "aireiq_alerta": ("1 si la proyeccion a 10 min supera el p95", None if pd.isna(r["alerta"]) else float(r["alerta"])),
            "aireiq_rmse_modelo": ("RMSE acumulado del modelo", math.sqrt(sq_m / n_ev) if n_ev else None),
            "aireiq_rmse_persistencia": ("RMSE acumulado de la persistencia", math.sqrt(sq_p / n_ev) if n_ev else None),
            "aireiq_lecturas_validas": ("Lecturas validas acumuladas", float(validas)),
            "aireiq_lecturas_invalidas": ("Lecturas invalidas acumuladas", float(invalidas)),
            "aireiq_evaluaciones": ("Proyecciones ya comparadas con lo real", float(n_ev)),
        }
        ultimo = texto_metricas(m)
        if a.resumen or a.seco:
            continue
        enviar(a.url, "aireiq_iaq", "iaq", ultimo)
        if a.pausa:
            time.sleep(a.pausa)
        if (i + 1) % 50 == 0:
            print(f"  publicados {i + 1}/{len(df)}")
    if a.resumen and not a.seco:
        enviar(a.url, "aireiq_iaq", "iaq", ultimo)
    if a.seco:
        print(ultimo)
    else:
        print("Listo.")


if __name__ == "__main__":
    main()
