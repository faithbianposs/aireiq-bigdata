# Humedad relativa · Eberth Emmanuel Rosales Castillo

Parte individual de AireIQ: **predecir la humedad media del ambiente a un horizonte de 120 segundos y emitir eventos empresariales de ventilación preventiva con cooldown si se superan los umbrales de control estadístico.**

## Preguntas

| Tipo | Pregunta | Dónde |
|---|---|---|
| Descriptiva (U1) | ¿Cómo varía la humedad por hora y día y qué proporción de lecturas queda fuera de p5–p95 (30.28%–42.97%)? | `notebooks/05_producto_u1_humedad.ipynb` |
| Predictiva (U2) | ¿Cuál será la humedad media del minuto objetivo que inicia 120 s después del minuto de entrada? | `notebooks/10_pronostico_humedad_120s.ipynb` y `services/streaming/live_forecast.py` |
| Decisión / Negocio | ¿Se anticipa un exceso de humedad que requiera ventilación preventiva antes de que ocurra? | Regla estadística (p95 = 42.0%), emisión a `aireiq-intervenciones-v1` con *cooldown* de 300 s y auditoría |

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

## Por qué horizonte a 120 segundos

En un entorno de streaming real, el minuto $t$ termina en $t+60\text{ s}$, pero debido al buffer de tolerancia de 10 s para agrupar lecturas físicas, el agregado del minuto se cierra en $t+70\text{ s}$. Predecir el minuto $t+1$ (que comenzó en $t+60\text{ s}$) llegaría tarde. El horizonte $t+2$ ($t+120\text{ s}$) garantiza una **anticipación real comprobada de $\approx 43.76\text{ s}$**.

## Resultados (Prueba cronológica y streaming en vivo)

| Etapa | Persistencia (RMSE) | Regresión lineal | Random Forest | Margen de anticipación |
|---|---|---|---|---|
| Validación cronológica (802 pares) | **0,5638 %** | 0,6244 % | 3,2603 % | Teórico 50 s |
| Test histórico independiente (804 pares) | **0,3313 %** | — | — | Teórico 50 s |
| Inferencia en vivo continua (sensor físico) | **1,7388 %** | — | — | **43,76 s real** |

Persistencia ganó en validación estricta y fue el modelo seleccionado para producción.

## Evento empresarial derivado y cooldown

- Si la humedad predicha supera el $p_{95}$ ($42.0\%$), se emite `accion: ventilacion_preventiva_sugerida` con `origen: pronostico_automatico`.
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
