"""Publica las métricas del streaming de presión a Pushgateway. Autor: Marco.

Lee data/gold/streaming_presion (salida de consumidor_presion.py) y envía, minuto a minuto: la presión
real, la que se proyectó 3 horas antes para ese minuto, la persistencia, el cambio previsto, el límite
de aviso, la alerta y los RMSE acumulados. Prometheus recoge esos valores y Grafana los dibuja.

Dentro del contenedor:
  python marco_presion/streaming/publicar_grafana_presion.py --pausa 0.2    # reproduce el tramo
  python marco_presion/streaming/publicar_grafana_presion.py --resumen      # solo los valores finales
  python marco_presion/streaming/publicar_grafana_presion.py --seguir       # modo vivo
"""
import argparse
import json
import math
import time
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
JOB, VARIABLE = "aireiq_presion", "presion"


def texto_metricas(m):
    lineas = []
    for nombre, (ayuda, valor) in m.items():
        if valor is None or (isinstance(valor, float) and math.isnan(valor)):
            continue
        lineas += [f"# HELP {nombre} {ayuda}", f"# TYPE {nombre} gauge", f"{nombre} {valor}"]
    return "\n".join(lineas) + "\n"


def enviar(url, cuerpo):
    req = urllib.request.Request(f"{url}/metrics/job/{JOB}/variable/{VARIABLE}", data=cuerpo.encode(),
                                 method="PUT", headers={"Content-Type": "text/plain; version=0.0.4"})
    urllib.request.urlopen(req, timeout=10).read()


def num(x):
    return None if pd.isna(x) else float(x)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://pushgateway:9091")
    ap.add_argument("--entrada", default=str(ROOT / "data" / "gold" / "streaming_presion"))
    ap.add_argument("--alerta", default=str(ROOT / "models" / "marco_presion" / "alerta_caida_3h.json"))
    ap.add_argument("--pausa", type=float, default=0.2, help="segundos entre minutos")
    ap.add_argument("--resumen", action="store_true", help="enviar solo los valores finales")
    ap.add_argument("--seco", action="store_true", help="no enviar, solo imprimir el último envío")
    ap.add_argument("--seguir", action="store_true", help="modo vivo: envía solo los minutos nuevos")
    ap.add_argument("--cada", type=float, default=10, help="segundos entre revisiones en modo --seguir")
    a = ap.parse_args()

    limite = json.loads(Path(a.alerta).read_text())["limite_aviso_hpa"] if Path(a.alerta).exists() else None

    def leer():
        return pd.read_parquet(a.entrada).sort_values("minuto").reset_index(drop=True)

    def filas_metricas(df):
        sq_m = sq_p = 0.0
        n_ev = validas = invalidas = 0
        for _, r in df.iterrows():
            validas += int(r["n_validas"])
            invalidas += int(r["n_invalidas"])
            if pd.notna(r["error_modelo"]):
                sq_m += float(r["error_modelo"]) ** 2
                sq_p += float(r["error_persistencia"]) ** 2
                n_ev += 1
            persist = (float(r["presion_real"]) - float(r["error_persistencia"])
                       if pd.notna(r["error_persistencia"]) and pd.notna(r["presion_real"]) else None)
            m = {
                "aireiq_presion_real": ("Presion medida en el minuto (hPa)", num(r["presion_real"])),
                "aireiq_presion_proyectada": ("Presion proyectada 3 h antes para este minuto (hPa)", num(r["proyeccion_previa"])),
                "aireiq_presion_persistencia": ("Presion de hace 3 h (persistencia, hPa)", persist),
                "aireiq_presion_cambio_previsto": ("Cambio de presion previsto para las proximas 3 h (hPa)", num(r["cambio_previsto"])),
                "aireiq_presion_limite_aviso": ("Limite de aviso de caida fuerte (hPa)", limite),
                "aireiq_presion_alerta": ("1 si se preve una caida fuerte en 3 h", num(r["alerta"])),
                "aireiq_presion_rmse_modelo": ("RMSE acumulado del modelo (hPa)", math.sqrt(sq_m / n_ev) if n_ev else None),
                "aireiq_presion_rmse_persistencia": ("RMSE acumulado de la persistencia (hPa)", math.sqrt(sq_p / n_ev) if n_ev else None),
                "aireiq_presion_evaluaciones": ("Proyecciones ya comparadas con lo real", float(n_ev)),
                "aireiq_presion_lecturas_validas": ("Lecturas validas acumuladas", float(validas)),
                "aireiq_presion_lecturas_invalidas": ("Lecturas invalidas acumuladas", float(invalidas)),
            }
            yield r["minuto"], texto_metricas(m)

    if a.seguir:
        print(f"Modo vivo | límite {limite} hPa | reviso cada {a.cada:.0f} s | Ctrl+C para parar")
        ultimo = None
        while True:
            try:
                filas = list(filas_metricas(leer()))
                nuevas = [f for f in filas if ultimo is None or f[0] > ultimo]
                if ultimo is None and nuevas:
                    nuevas = nuevas[-1:]          # arrancar en el estado actual, sin reproducir el pasado
                for minuto, cuerpo in nuevas:
                    enviar(a.url, cuerpo)
                    ultimo = minuto
                if nuevas:
                    print(f"{time.strftime('%H:%M:%S')} publicados {len(nuevas)} minutos (hasta {ultimo})")
                time.sleep(a.cada)
            except KeyboardInterrupt:
                print("Detenido.")
                return
            except Exception as e:  # parquet a medio escribir, pushgateway reiniciando, etc.
                print(f"{time.strftime('%H:%M:%S')} aviso: {type(e).__name__}: {e} (reintento)")
                time.sleep(a.cada)

    df = leer()
    print(f"{len(df)} minutos | límite de aviso {limite} hPa")
    ultimo = None
    for i, (minuto, cuerpo) in enumerate(filas_metricas(df)):
        ultimo = cuerpo
        if a.resumen or a.seco:
            continue
        enviar(a.url, cuerpo)
        if a.pausa:
            time.sleep(a.pausa)
        if (i + 1) % 100 == 0:
            print(f"  publicados {i + 1}/{len(df)}")
    if a.resumen and not a.seco:
        enviar(a.url, ultimo)
    print(ultimo if a.seco else "Listo.")


if __name__ == "__main__":
    main()
