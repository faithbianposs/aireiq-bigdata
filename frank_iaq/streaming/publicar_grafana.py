"""Publica las métricas del streaming de IAQ a Pushgateway (S11).

Lee data/gold/streaming_iaq (salida del consumidor) y envía, minuto a minuto, el IAQ real,
la proyección que se hizo 10 minutos antes, la persistencia, el umbral p95, la alerta y los
RMSE acumulados. Prometheus recoge esos valores de Pushgateway y Grafana los dibuja.

Dentro del contenedor:
  python frank_iaq/streaming/publicar_grafana.py --pausa 0.5      # reproduce el tramo en vivo
  python frank_iaq/streaming/publicar_grafana.py --resumen        # solo los valores finales
  python frank_iaq/streaming/publicar_grafana.py --seguir         # modo vivo: envía los minutos nuevos
"""
import argparse
import math
import time
import traceback
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
    ap.add_argument("--seguir", action="store_true",
                    help="modo vivo: vigila la salida del consumidor y envía solo los minutos nuevos")
    ap.add_argument("--cada", type=float, default=10, help="segundos entre revisiones en modo --seguir")
    ap.add_argument("--desde-inicio", action="store_true",
                    help="en --seguir, reproducir primero todo lo ya calculado")
    ap.add_argument("--desde", default=None,
                    help="contar solo minutos desde esta fecha (ej. 2026-11-17); excluye el reemplazo antiguo")
    a = ap.parse_args()

    p95 = float(pd.read_parquet(a.umbral)["p95"].iloc[0])

    def leer():
        d = pd.read_parquet(a.entrada).sort_values("minuto").reset_index(drop=True)
        if a.desde:
            d = d[d["minuto"] >= pd.Timestamp(a.desde)].reset_index(drop=True)
        return d

    def filas_metricas(df):
        """Recorre el DataFrame acumulando RMSE y lecturas; devuelve (minuto, texto) por fila."""
        sq_m = sq_p = 0.0
        n_ev = 0
        validas = invalidas = 0
        for _, r in df.iterrows():
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
            yield r["minuto"], texto_metricas(m)

    if a.seguir:
        print(f"Modo vivo | p95 = {p95:.2f} | reviso cada {a.cada:.0f} s | Ctrl+C para parar")
        ultimo_min = None
        primera = True
        while True:
            try:
                filas = list(filas_metricas(leer()))
                if primera and filas and not a.desde_inicio:
                    # arrancar en el estado actual, sin reproducir el pasado
                    enviar(a.url, "aireiq_iaq", "iaq", filas[-1][1])
                    ultimo_min = filas[-1][0]
                    print(f"{time.strftime('%H:%M:%S')} estado actual publicado ({ultimo_min})")
                else:
                    nuevas = [f for f in filas if ultimo_min is None or f[0] > ultimo_min]
                    for minuto, cuerpo in nuevas:
                        enviar(a.url, "aireiq_iaq", "iaq", cuerpo)
                        ultimo_min = minuto
                        time.sleep(0.2)
                    if nuevas:
                        print(f"{time.strftime('%H:%M:%S')} publicados {len(nuevas)} minutos nuevos (hasta {ultimo_min})")
                primera = False
            except KeyboardInterrupt:
                print("Detenido.")
                return
            except Exception as e:  # parquet a medio escribir, pushgateway reiniciando, etc.
                print(f"{time.strftime('%H:%M:%S')} aviso: {type(e).__name__}: {e} (reintento)")
            try:
                time.sleep(a.cada)
            except KeyboardInterrupt:
                print("Detenido.")
                return
        return

    df = leer()
    print(f"{len(df)} minutos | p95 = {p95:.2f}")
    ultimo = None
    for i, (minuto, cuerpo) in enumerate(filas_metricas(df)):
        ultimo = cuerpo
        if a.resumen or a.seco:
            continue
        enviar(a.url, "aireiq_iaq", "iaq", cuerpo)
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
