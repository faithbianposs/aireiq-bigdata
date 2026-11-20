"""Publicador Grafana/Pushgateway - AireIQ Temperatura"""
import json, time
from pathlib import Path
import pandas as pd
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

ROOT           = Path(__file__).resolve().parents[2]
GOLD_DIR       = ROOT / "data" / "gold" / "streaming_temperatura"
METRICAS_BATCH = ROOT / "models" / "wrangler_temperatura" / "metricas.json"
GATEWAY        = "pushgateway:9091"
JOB            = "aireiq_wrangler_temperatura"
INTERVALO      = 30

def safe_int(val):
    try:
        return int(val) if pd.notna(val) else 0
    except:
        return 0

def safe_float(val):
    try:
        return float(val) if pd.notna(val) else None
    except:
        return None

def publicar():
    reg      = CollectorRegistry()
    g_temp   = Gauge("aireiq_temperatura_c",             "Temperatura real promedio del minuto",      registry=reg)
    g_p15    = Gauge("aireiq_temperatura_proy_15m",      "Proyeccion temperatura +15 min",            registry=reg)
    g_p30    = Gauge("aireiq_temperatura_proy_30m",      "Proyeccion temperatura +30 min",            registry=reg)
    g_p60    = Gauge("aireiq_temperatura_proy_60m",      "Proyeccion temperatura +60 min",            registry=reg)
    g_alerta = Gauge("aireiq_temperatura_alerta_p95",    "Alerta P95 temperatura (0/1)",              registry=reg)
    g_err_m  = Gauge("aireiq_temperatura_error_modelo",  "Error absoluto modelo ultimo minuto",       registry=reg)
    g_err_p  = Gauge("aireiq_temperatura_error_persist", "Error absoluto persistencia ultimo minuto", registry=reg)
    g_n_val  = Gauge("aireiq_temperatura_n_validas",     "Lecturas validas en el minuto",             registry=reg)
    g_n_inv  = Gauge("aireiq_temperatura_n_invalidas",   "Lecturas invalidas (gas_ohms=0)",           registry=reg)
    g_rmse15 = Gauge("aireiq_temperatura_rmse_15m",      "RMSE batch horizonte 15 min",               registry=reg)
    g_rmse30 = Gauge("aireiq_temperatura_rmse_30m",      "RMSE batch horizonte 30 min",               registry=reg)
    g_rmse60 = Gauge("aireiq_temperatura_rmse_60m",      "RMSE batch horizonte 60 min",               registry=reg)
    g_p5     = Gauge("aireiq_temperatura_umbral_p5",     "Umbral P5 temperatura C",                   registry=reg)
    g_p95    = Gauge("aireiq_temperatura_umbral_p95",    "Umbral P95 temperatura C",                  registry=reg)

    if METRICAS_BATCH.exists():
        m  = json.loads(METRICAS_BATCH.read_text())
        h  = m.get("horizontes", {})
        g_rmse15.set(h.get("15m", {}).get("rmse_modelo", 0))
        g_rmse30.set(h.get("30m", {}).get("rmse_modelo", 0))
        g_rmse60.set(h.get("60m", {}).get("rmse_modelo", 0))

    umbrales_path = ROOT / "models" / "wrangler_temperatura" / "umbrales.json"
    if umbrales_path.exists():
        u = json.loads(umbrales_path.read_text())
        g_p5.set(u.get("p5", 0))
        g_p95.set(u.get("p95", 0))

    print(f"Publicando en {GATEWAY} (job={JOB}) cada {INTERVALO}s - Ctrl+C para detener")

    while True:
        try:
            if GOLD_DIR.exists():
                archivos = sorted(GOLD_DIR.glob("**/*.parquet"))
                if archivos:
                    df = pd.read_parquet(archivos)
                    df_valido = df.dropna(subset=["temp_real"])
                    if not df_valido.empty:
                        ult  = df_valido.sort_values("minuto").iloc[-1]
                        temp = float(ult["temp_real"])
                        g_temp.set(temp)
                        v15 = safe_float(ult.get("temp_proy_15m"))
                        v30 = safe_float(ult.get("temp_proy_30m"))
                        v60 = safe_float(ult.get("temp_proy_60m"))
                        if v15 is not None: g_p15.set(v15)
                        if v30 is not None: g_p30.set(v30)
                        if v60 is not None: g_p60.set(v60)
                        g_alerta.set(safe_int(ult.get("alerta_p95")))
                        em = safe_float(ult.get("error_modelo"))
                        ep = safe_float(ult.get("error_persistencia"))
                        if em is not None: g_err_m.set(em)
                        if ep is not None: g_err_p.set(ep)
                        g_n_val.set(safe_int(ult.get("n_validas")))
                        g_n_inv.set(safe_int(ult.get("n_invalidas")))
                        push_to_gateway(GATEWAY, job=JOB, registry=reg)
                        proy = f"{v15:.2f}" if v15 is not None else "N/A"
                        print(f"  publicado | {ult['minuto']} | temp={temp:.2f}C | proy_15m={proy} | alerta={safe_int(ult.get('alerta_p95'))}")
        except Exception as e:
            print(f"  error: {e}")
        time.sleep(INTERVALO)

if __name__ == "__main__":
    publicar()
