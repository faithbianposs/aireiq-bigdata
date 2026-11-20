"""Prueba de no regresión · presión barométrica (Marco).

Comprueba que, después de cualquier cambio en el proyecto:
  1. las métricas guardadas siguen mostrando que el modelo le gana a la persistencia;
  2. el límite de aviso existe y representa una caída (valor negativo);
  3. el modelo guardado carga sin reentrenar y predice presiones físicamente razonables;
  4. el modelo usa la hora del día (predicciones distintas a distintas horas);
  5. el publicador arma bien el texto que recibe Pushgateway.

Uso, dentro del contenedor de Jupyter, desde la raíz del repositorio:
  python marco_presion/tests/test_presion.py
Requiere haber ejecutado antes marco_presion/notebooks/02_presion_modelos.ipynb.
"""
import importlib.util
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELOS = ROOT / "models" / "marco_presion"
FALLAS = []


def comprobar(nombre, condicion, detalle=""):
    print(("OK    " if condicion else "FALLA ") + nombre + (f" · {detalle}" if detalle else ""))
    if not condicion:
        FALLAS.append(nombre)


def fila(presion, lag60, hora):
    d = 2 * math.pi
    return dict(presion_hpa=presion, presion_lag60=lag60, presion_d60=presion - lag60,
                h24_sin=math.sin(d * hora / 24), h24_cos=math.cos(d * hora / 24),
                h12_sin=math.sin(d * hora / 12), h12_cos=math.cos(d * hora / 12))


# 1. Métricas guardadas
met = json.loads((MODELOS / "metricas_regresion.json").read_text())
for h, g in met["ganadores"].items():
    comprobar(f"el modelo a {h} min supera a la persistencia", g["supera_persistencia"],
              f"{g['modelo']} RMSE {g['rmse']:.3f} vs {g['rmse_persistencia']:.3f} hPa")

# 2. Límite de aviso
alerta = json.loads((MODELOS / "alerta_caida_3h.json").read_text())
comprobar("el límite de aviso es una caída (negativo)", alerta["limite_aviso_hpa"] < 0,
          f"{alerta['limite_aviso_hpa']} hPa")

# 3 y 4. Inferencia con el modelo guardado
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("test-presion").master("local[1]")
         .config("spark.driver.memory", "1g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
modelo = PipelineModel.load(str(MODELOS / "presion_h180"))
casos = spark.createDataFrame([fila(645.0, 645.0, h) for h in (4, 9, 15, 21)])
pred = [r["prediction"] for r in modelo.transform(casos).select("prediction").collect()]
comprobar("predicciones dentro de un rango físico (620–670 hPa)", all(620 < p < 670 for p in pred),
          ", ".join(f"{p:.2f}" for p in pred))
comprobar("el modelo usa la hora del día", max(pred) - min(pred) > 0.1,
          f"diferencia entre horas {max(pred) - min(pred):.2f} hPa")
spark.stop()

# 5. Publicador
spec = importlib.util.spec_from_file_location("publicar", ROOT / "marco_presion" / "streaming" / "publicar_grafana_presion.py")
publicar = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publicar)
texto = publicar.texto_metricas({"aireiq_presion_real": ("Presion", 645.1), "aireiq_presion_alerta": ("Alerta", None)})
comprobar("el publicador escribe la métrica y omite los valores vacíos",
          "aireiq_presion_real 645.1" in texto and "aireiq_presion_alerta" not in texto)

print("\nResultado:", "TODO OK" if not FALLAS else f"{len(FALLAS)} falla(s): {FALLAS}")
sys.exit(1 if FALLAS else 0)
