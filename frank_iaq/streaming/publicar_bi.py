"""Publica a Pushgateway las tablas de BI de Frank (IAQ): lo que ya está calculado en lotes.

Lee:
  data/gold/iaq_fuera_p95_por_hora, data/gold/iaq_fuera_p95_por_dia, data/gold/umbral_iaq
  models/frank_iaq/metricas_regresion.json, models/frank_iaq/metricas_alerta.json
y las envía como métricas con etiquetas (hora, fecha, horizonte, modelo, version) al job
aireiq_iaq_bi. El tablero «AireIQ · IAQ · BI y modelos» las dibuja.

Pushgateway guarda todo en memoria: si reinicias los contenedores, vuelve a ejecutarlo.

  python frank_iaq/streaming/publicar_bi.py            # dentro del contenedor jupyter
  python frank_iaq/streaming/publicar_bi.py --seco     # solo imprime
"""
import argparse
import json
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"')


class Salida:
    def __init__(self):
        self.lineas = []
        self.vistos = set()

    def gauge(self, nombre, ayuda, valor, **etiquetas):
        if valor is None or pd.isna(valor):
            return
        if nombre not in self.vistos:
            self.lineas += [f"# HELP {nombre} {ayuda}", f"# TYPE {nombre} gauge"]
            self.vistos.add(nombre)
        et = ",".join(f'{k}="{esc(v)}"' for k, v in etiquetas.items())
        self.lineas.append(f"{nombre}{{{et}}} {float(valor)}" if et else f"{nombre} {float(valor)}")

    def texto(self):
        return "\n".join(self.lineas) + "\n"


def construir(raiz):
    s = Salida()
    gold, modelos = raiz / "data" / "gold", raiz / "models" / "frank_iaq"

    hora = pd.read_parquet(gold / "iaq_fuera_p95_por_hora")
    for _, r in hora.iterrows():
        h = f"{int(r['hora']):02d}"
        s.gauge("aireiq_bi_pct_fuera_p95_hora", "% de minutos sobre el p95 por hora del dia", r["pct_fuera_p95"], hora=h)
        s.gauge("aireiq_bi_iaq_prom_hora", "IAQ promedio por hora del dia", r["iaq_prom"], hora=h)

    dia = pd.read_parquet(gold / "iaq_fuera_p95_por_dia")
    for _, r in dia.iterrows():
        f = str(r["fecha"])
        s.gauge("aireiq_bi_pct_fuera_p95_dia", "% de minutos sobre el p95 por dia", r["pct_fuera_p95"], fecha=f)
        s.gauge("aireiq_bi_minutos_dia", "Minutos validos por dia", r["minutos"], fecha=f)

    s.gauge("aireiq_bi_minutos_total", "Minutos validos en total", dia["minutos"].sum())
    s.gauge("aireiq_bi_pct_fuera_p95_total", "% de minutos sobre el p95 (todo el historico)",
            100 * dia["min_fuera_p95"].sum() / max(dia["minutos"].sum(), 1))
    s.gauge("aireiq_bi_umbral_p95", "Umbral p95 del periodo de referencia",
            float(pd.read_parquet(gold / "umbral_iaq")["p95"].iloc[0]))

    reg = json.loads((modelos / "metricas_regresion.json").read_text(encoding="utf-8"))
    for r in reg["tabla"]:
        et = dict(horizonte=f"{int(r['horizonte']):02d} min", modelo=r["modelo"], version=r["version"])
        s.gauge("aireiq_bi_rmse", "RMSE en el conjunto de prueba", r["rmse"], **et)
        s.gauge("aireiq_bi_mae", "MAE en el conjunto de prueba", r["mae"], **et)
        s.gauge("aireiq_bi_r2", "R2 en el conjunto de prueba", r["r2"], **et)
        s.gauge("aireiq_bi_mejora_pct", "Mejora % del RMSE frente a la persistencia (negativo = peor)",
                r["mejora_vs_persistencia_%"], **et)
    for h, g in reg["ganadores"].items():
        s.gauge("aireiq_bi_ganador_rmse", "RMSE del mejor modelo", g["rmse"], horizonte=f"{int(h):02d} min", modelo=g["modelo"])
        s.gauge("aireiq_bi_ganador_rmse_persistencia", "RMSE de la persistencia", g["rmse_persistencia"], horizonte=f"{int(h):02d} min")
        s.gauge("aireiq_bi_ganador_mejora_pct", "Mejora % del ganador frente a la persistencia", g["mejora_pct"], horizonte=f"{int(h):02d} min")

    ale = json.loads((modelos / "metricas_alerta.json").read_text(encoding="utf-8"))
    for r in ale["tabla_todos"]:
        for m in ("precision", "recall", "f1"):
            s.gauge(f"aireiq_bi_alerta_{m}", f"{m} de la alerta a 60 min", r[m], modelo=r["modelo"])
    s.gauge("aireiq_bi_deterioros_anticipables", "Deterioros anticipables en el conjunto de prueba",
            ale.get("deterioros_anticipables"))
    return s.texto()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://pushgateway:9091")
    ap.add_argument("--seco", action="store_true")
    a = ap.parse_args()
    cuerpo = construir(ROOT)
    n = sum(1 for l in cuerpo.splitlines() if l and not l.startswith("#"))
    if a.seco:
        print(cuerpo)
        print(f"{n} series")
        return
    req = urllib.request.Request(f"{a.url}/metrics/job/aireiq_iaq_bi", data=cuerpo.encode(), method="PUT",
                                 headers={"Content-Type": "text/plain; version=0.0.4"})
    urllib.request.urlopen(req, timeout=10).read()
    print(f"Publicadas {n} series de BI en {a.url}")


if __name__ == "__main__":
    main()
