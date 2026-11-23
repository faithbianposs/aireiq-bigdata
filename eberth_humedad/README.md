# Humedad relativa · Eberth Emmanuel Rosales Castillo

Parte individual de AireIQ: **predecir la humedad media del ambiente a un horizonte de 120 segundos y emitir eventos empresariales de ventilación preventiva con cooldown si se superan los umbrales de control estadístico.**

## Preguntas

| Tipo | Pregunta | Dónde |
|---|---|---|
| Descriptiva (U1) | ¿Cómo varía la humedad por hora y día y qué proporción de lecturas queda fuera de p5–p95 (30.28%–42.97%)? | `notebooks/05_producto_u1_humedad.ipynb` |
| Predictiva (U2) | ¿Cuál será la humedad media del minuto objetivo que inicia 120 s después del minuto de entrada? | `notebooks/10_pronostico_humedad_120s.ipynb` y `services/streaming/live_forecast.py` |
| Decisión / Negocio | ¿La predicción supera el umbral configurado para solicitar una revisión? | Umbral de demostración de 42,0%, emisión a `aireiq-intervenciones-v1` con *cooldown* de 300 s y auditoría |

## Arquitectura y flujo

```text
ESP32 (BME680) → Supabase REST → puente polling → Kafka (aireiq-mediciones-supabase-v1)
                            → adaptador minutos vivos (tolerancia 10 s) → Kafka (aireiq-minutos-vivos-v1)
                            → Spark Structured Streaming + MLlib PipelineModel (horizonte 120 s)
                            → Evaluación en SQLite (evaluation.sqlite)
                            → Pushgateway → Prometheus → Grafana (aireiq_humedad.json)
                            → Alerta preventiva (cooldown 300 s) → Kafka (aireiq-intervenciones-v1)
                            → Worker auditor → Kafka (aireiq-intervenciones-auditadas-v1)
```

## Cómo levantarlo

Desde `eberth_humedad/` (copiar `.env.example` a `.env` y colocar `mediciones_aire_rows.csv` en esta carpeta):

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml -f compose.supabase.yaml -f compose.live.yaml --profile live --profile supabase up -d --build --wait
```

El servicio de inferencia requiere un modelo entrenado en `outputs/s10_h120/`, que no se publica en Git. En una instalación nueva, ejecutar primero los cuadernos de U1 y `10_pronostico_humedad_120s.ipynb` para generar Gold y el modelo; después iniciar el flujo vivo. Configurar Supabase y Grafana en `.env`. Algunos scripts de demostración usan la ruta del Python de la máquina original: adaptar esa ruta o ejecutar los cuadernos desde JupyterLab. No se ha verificado una instalación limpia en otro equipo.

| Servicio | URL local |
|---|---|
| JupyterLab | http://localhost:4488 |
| Kafka UI | http://localhost:48085 |
| Prometheus | http://localhost:49090 |
| Pushgateway | http://localhost:49091 |
| kafka-exporter | http://localhost:49308/metrics |
| Grafana (tablero de humedad) | http://localhost:43000/d/aireiq-vivo |

Kafka llega a Grafana por dos vías: **kafka-exporter → Prometheus** (brokers, mensajes/s, mensajes por partición y lag) y **Spark `live_forecast.py` → Pushgateway → Prometheus** (predicción, RMSE/MAE vivo, anticipación e intervenciones). Ambas usan el datasource `aireiq-prometheus` provisionado en `infra/grafana/`.

## Por qué horizonte a 120 segundos

El minuto de entrada termina en t+60 s y el adaptador espera diez segundos adicionales. El objetivo t+120 s permite anticipación si la inferencia termina antes de ese instante; no la garantiza ante retrasos. La verificación guardada del 20 de noviembre de 2026 registró un margen de 43,76 s para la última predicción oportuna. Los resultados tardíos se excluyen de la evaluación de anticipación.

## Resultados (Prueba cronológica y streaming en vivo)

| Etapa | Persistencia (RMSE) | Regresión lineal | Random Forest | Margen de anticipación |
|---|---|---|---|---|
| Validación cronológica (802 pares) | **0,5639** | 0,6245 | 3,2604 | Teórico 50 s, menos inferencia |
| Test histórico independiente (804 pares) | **0,3314** | — | — | Teórico 50 s, menos inferencia |
| Muestra viva del 20/11/2026 (5 pares evaluados) | **1,7388** | — | — | **43,76 s en la última predicción oportuna** |

RMSE está expresado en puntos porcentuales de humedad, no en porcentaje de precisión. Persistencia ganó en validación y fue seleccionada para el servicio; no hay mejora frente a esa misma referencia. La muestra viva es pequeña y usa agregados posiblemente incompletos. La evidencia guardada no acredita que los servicios estén activos hoy. Ver [verificación](evidencias/verification.json).

## Evento empresarial derivado y cooldown

- Si la humedad predicha supera el umbral configurado (42,0% por defecto), se emite `accion: ventilacion_preventiva_sugerida` con `origen: pronostico_automatico`. Ese valor no es el p95 histórico exacto de U1 (42,969675%); es un umbral de demostración y no una recomendación sanitaria ni un control físico de ventilación.
- **Mecanismo de cooldown (300 s):** Evita saturar Kafka con alertas redundantes en minutos sucesivos si la humedad permanece alta.
- **Auditoría:** El worker `eventos` valida el contrato v1 y emite la confirmación a `aireiq-intervenciones-auditadas-v1`.

## Estructura de archivos

| Directorio / Archivo | Contenido |
|---|---|
| `notebooks/` | Cuadernos limpios de U1 (calidad, Parquet, ML contemporáneo) y U2 (pronóstico a 120 s con MLlib). |
| `services/` | Microservicios en Python: puente Supabase, adaptador de minutos, inferencia Spark Streaming y auditor de eventos. |
| `scripts/` | Scripts de verificación y demostración visible en PowerShell y Python. |
| `docs/` | Documentación técnica completa, matrices de evidencias (`u1-producto.md`, `u2-producto.md`) y guiones de sustentación (`guion-s05.md`, `guion-s12.md`). |
| `evidencias/` | Manifiestos JSON con status `passed` (`manifest_u2_integrado.json`, `manifest_evento_empresarial.json`, `manifest_resiliencia.json`). |
| `../grafana/dashboards/aireiq_humedad.json` | Tablero de Grafana con anticipación real, error vivo y métricas de intervenciones. |
