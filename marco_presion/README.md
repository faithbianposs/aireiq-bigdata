# Presión barométrica · Marco (stefano159jk)

Parte individual de AireIQ: **predecir la presión barométrica dentro de 3 horas y avisar si se anticipa una caída fuerte.**

## Preguntas

| Tipo | Pregunta | Dónde |
|---|---|---|
| Descriptiva (U1) | ¿Cómo se comporta la presión a lo largo del día y entre días? | `notebooks/01_presion_oro.ipynb` |
| Predictiva (U2) | ¿Cuál será la presión dentro de 1 hora y de 3 horas? | `notebooks/02_presion_modelos.ipynb` |
| Decisión | ¿Se anticipa una caída fuerte de presión en las próximas 3 horas? | Cuaderno 02 (sección 6) y streaming |

## Datos

- Entrada: la **plata** del equipo (`data/silver/mediciones_1min`), que genera `frank_iaq/notebooks/01_bronce_plata_oro.ipynb` a partir de `data/raw/mediciones_aire.csv`.
- La presión del BME680 está en hPa. En Juliaca (~3 800 m) ronda los 645 hPa.
- Se descartan las lecturas con `gas_ohms = 0` (sensor congelado) y fuera de rango físico: son las mismas reglas del equipo.

## Cómo ejecutar

Desde la raíz del repositorio, con el CSV en `data/raw/mediciones_aire.csv`:

```powershell
docker compose up -d jupyter
```

En JupyterLab (`http://localhost:4488/lab?token=aireiq`), ejecutar en este orden con *Run All Cells*:

1. `frank_iaq/notebooks/01_bronce_plata_oro.ipynb` → genera la plata.
2. `marco_presion/notebooks/01_presion_oro.ipynb` → oro de presión en `data/gold/`.
3. `marco_presion/notebooks/02_presion_modelos.ipynb` → modelos en `models/marco_presion/`.

Streaming y Grafana, desde una terminal de JupyterLab (*File → New → Terminal*):

```bash
# a) Sin Kafka (reproduce el histórico como archivos)
python frank_iaq/streaming/productor_csv.py --destino archivos
spark-submit marco_presion/streaming/consumidor_presion.py --fuente archivos --una-vez

# b) Con Kafka (en vivo con el puente de Supabase o con el productor)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    marco_presion/streaming/consumidor_presion.py --fuente kafka --servidores kafka:19092

# Publicar a Grafana (requiere: docker compose up -d pushgateway prometheus grafana)
python marco_presion/streaming/publicar_grafana_presion.py --pausa 0.2   # reproducir
python marco_presion/streaming/publicar_grafana_presion.py --seguir      # modo vivo
```

Tablero: `http://localhost:43000` → Dashboards → AireIQ → **AireIQ · Presión (Marco)**.

Prueba de no regresión:

```bash
python marco_presion/tests/test_presion.py
```

## Resultados (prueba cronológica, 20 % más reciente)

| Horizonte | Persistencia (RMSE) | Mejor modelo | RMSE | Mejora |
|---|---|---|---|---|
| 1 hora | 0,241 hPa | Ridge | 0,188 hPa | 22 % |
| 3 horas | 1,265 hPa | Ridge | 0,544 hPa | 57 % |

El modelo gana porque conoce la hora del día (ciclos de 24 y 12 horas) y anticipa la **marea atmosférica**: la presión sube y baja dos veces al día a horas parecidas.

**Alerta de caída fuerte:** caída real de 2,51 hPa o más en 3 horas (p5 del entrenamiento). El límite de aviso se calibró en entrenamiento: avisar si la caída prevista es de 1,95 hPa o más. Prueba: 35 de 35 caídas avisadas, 0 avisos de más (un solo episodio).

**Streaming (histórico reproducido):** RMSE 0,362 hPa del modelo frente a 1,539 hPa de la persistencia en 798 comparaciones (incluye el periodo de entrenamiento).

## Archivos

| Archivo | Qué hace |
|---|---|
| `notebooks/01_presion_oro.ipynb` | Presión por hora (marea), por día (rango diario) y cambio en 3 h |
| `notebooks/02_presion_modelos.ipynb` | Modelos a 1 h y 3 h, guardado, inferencia y alerta calibrada |
| `streaming/consumidor_presion.py` | Spark Structured Streaming: ventanas de 1 min, watermark, inferencia con el modelo guardado |
| `streaming/publicar_grafana_presion.py` | Envía las métricas a Pushgateway → Prometheus → Grafana |
| `tests/test_presion.py` | Prueba de no regresión |
| `../grafana/dashboards/aireiq_presion.json` | Tablero de Grafana |
| `evidencias/` | Capturas de la ejecución |

## Límites

- Pocos días de datos y con huecos; un solo sensor.
- Sin datos de lluvia: la alerta es una señal de cambio de tiempo, no una predicción de lluvia.
- Las cifras deben volver a medirse cuando haya más días en Supabase.
