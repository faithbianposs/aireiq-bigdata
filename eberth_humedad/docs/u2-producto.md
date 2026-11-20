# AireIQ: producto U2 de humedad

Responsable: Eberth Emmanuel Rosales Castillo. Pregunta predictiva: ¿cuál será la humedad media del siguiente minuto (o del minuto objetivo con horizonte a 120 s) en el dispositivo observado? El resultado ayuda a observar cambios y desviaciones estadísticas; no constituye una recomendación sanitaria.

## Arquitectura ejecutada

```text
Flujo histórico / reproducción (S6-S11):
CSV exportado de Supabase → productor IoT → Kafka → Spark → ventanas Parquet
Eventos de intervención simulados → Kafka → validador → auditoría / inválidos
Gold de U1 → entrenamiento cronológico → PipelineModel
Minutos Gold de prueba → Kafka → Spark + PipelineModel → evaluación SQLite
Evaluación → Pushgateway → Prometheus → Grafana BI

Flujo continuo en vivo de extremo a extremo (S12):
ESP32 (BME680) → Supabase → puente polling → Kafka (aireiq-mediciones-supabase-v1)
                           → adaptador minutos vivos (tolerancia 10 s) → Kafka (aireiq-minutos-vivos-v1)
                           → Spark Structured Streaming + MLlib PipelineModel (horizonte 120 s)
                           → Evaluación oportuna en SQLite (outputs/live_forecast/evaluation.sqlite)
                           → Pushgateway → Prometheus → Grafana vivo (http://localhost:43000/d/aireiq-vivo)
                           → Evento empresarial preventivo (cooldown 300 s) → Kafka (aireiq-intervenciones-v1)
                           → Consumidor auditor → Kafka (aireiq-intervenciones-auditadas-v1)
```

La arquitectura unifica la ruta histórica batch/reproducción con la ruta continua viva. El puente de lectura consulta Supabase de forma segura en solo lectura, el adaptador cierra minutos con 10 s de gracia y emite a Kafka, y el servicio de inferencia Spark predice a un horizonte de 120 segundos. Esto resuelve el problema de disponibilidad temporal: como el minuto $t$ se cierra en $t+70\text{ s}$, predecir $t+1$ ($t+60\text{ s}$) llegaría tarde; en cambio, el horizonte $t+2$ ($t+120\text{ s}$) garantiza una anticipación real de $\approx 43.7\text{ s}$.

## Capacidades y evidencia

| Sesión | Implementación AireIQ | Evidencia local | Límite |
|---|---|---|---|
| S6 | Intervenciones con contrato, validación, auditoría e inválidos | `outputs/s06/`, `docs/s06-eventos.md` | Python; no reproduce los dos microservicios Spring del laboratorio |
| S7 | Reproducción CSV con clave de dispositivo | `outputs/s07/`, `docs/s07-iot.md` | No es ingesta continua desde Supabase ni puente MQTT |
| S8 | Ventanas, watermark, deduplicación y recuperación | `outputs/s08/`, `outputs/s08_historico/` | Un broker y Spark local; no acredita alta disponibilidad |
| S9 | Métricas reales, alertas y recursos estimados | `outputs/s09/`, `docs/s09-costos.md` | Sin notificaciones externas ni cotización cloud |
| S10 | Comparación cronológica y modelo persistido | `outputs/s10/`, `outputs/s10_streaming/` | Persistencia ganó; no hay mejora sobre ella |
| S11 | Predicción, humedad observada y error al llegar el valor real | `outputs/s11/`, `docs/s11-bi.md` | Replay acelerado; tablero comprobado por API |
| S12 | Inferencia continua en vivo (120 s), evaluación oportuna, eventos preventivos con cooldown y resiliencia | `outputs/live_forecast/`, `docs/pronostico-vivo.md`, `docs/guion-s12.md` | Sensor físico único; agregados sujetos a 10 s de gracia |

El test completo S10 contiene 823 pares: RMSE 0,21926 y MAE 0,08731 puntos porcentuales. La inferencia en vivo con horizonte de 120 segundos (`outputs/live_forecast/verification.json`, status="passed") acumula 18 observaciones reales con 9 predicciones oportunas y 5 pares ya evaluados al llegar el valor real, con RMSE $\approx 1.74$ y margen de anticipación de $43.76\text{ s}$. El error de U1 corresponde a estimación contemporánea y no debe compararse como una mejora directa con U2.

## Inferencia en vivo y cierre de la lógica funcional (S12)

1. **Horizonte a 120 segundos y anticipación estricta**: Cada emisión calcula predicción para `minuto + 120 s`. Solo se consideran oportunas las emisiones anteriores a ese inicio objetivo. Los minutos históricos o tardíos no suman anticipación.
2. **Evaluación no destructiva e idempotente**: Las predicciones y observaciones se emparejan por `(device, target_minute)` exacto en `evaluation.sqlite`. No se inventan valores ni se rellenan minutos ausentes.
3. **Evento empresarial derivado con cooldown**: Si la humedad predicha excede los umbrales de control estadístico ($p_{95} = 42.0\%$ o $p_5 = 30.0\%$ de U1), se genera una alerta/intervención `ventilacion_preventiva_sugerida` con `origen=pronostico_automatico` a `aireiq-intervenciones-v1`. Un mecanismo de enfriamiento (*cooldown* de 300 s) suprime emisiones repetitivas mientras el valor permanezca fuera de rango, preservando la trazabilidad sin saturar Kafka.
4. **Resiliencia ante caídas e idempotencia**: Se verificó la recuperación del contenedor (`outputs/live_forecast/manifest_resiliencia.json`, status="passed"). Las claves primarias en SQLite impiden duplicaciones y el checkpoint de Spark Structured Streaming preserva los offsets confirmados sin reprocesar de forma destructiva.

## Preparación de la demostración

Abrir PowerShell en la raíz del proyecto. Los scripts de demostración visible contienen los comandos probados:
- `scripts/demo_s06_visible.ps1`: eventos empresariales y auditoría.
- `scripts/demo_s07_visible.ps1`: ingesta IoT con clave de partición.
- `scripts/demo_s08_visible.ps1`: ventanas Spark y watermark.
- `scripts/demo_s09_visible.ps1`: observabilidad Prometheus/Grafana y costos.
- `scripts/demo_s11_visible.ps1`: tablero BI de evaluación por replay.
- `scripts/demo_s12_ventanas_visible.ps1`: ventanas deslizantes de 5 min desplazadas cada min.
- `scripts/iniciar_pronostico_vivo_visible.ps1`: arranque del flujo vivo continuo con Supabase y Spark MLlib.
- `scripts/verificar_pronostico_vivo_visible.ps1`: verificación por API contra el datasource de Grafana (`status=passed`).
- `scripts/verificar_evento_pronostico_visible.ps1`: verificación del evento empresarial derivado de predicción (`status=passed`).

Para la defensa: consultar el guion completo en [docs/guion-s12.md](guion-s12.md). Mostrar origen y contrato del evento, su topic/partición/offset, el lag del grupo consumidor, la anticipación real en Grafana y la auditoría del evento derivado. Explicar por qué persistencia es difícil de batir en series temporales de alta autocorrelación a corto plazo y cómo la clave de partición asegura orden determinista por dispositivo.

## Estado final y pendientes de entrega

- [x] Ingesta de eventos empresariales (S6) con contrato v1 y auditoría.
- [x] Ingesta IoT (S7) con particionamiento por dispositivo y validación.
- [x] Streaming con ventanas fijas y deslizantes con watermark (S8/S12).
- [x] Observabilidad con métricas de lag, alertas y costos estimados (S9).
- [x] Modelo predictivo temporal evaluado frente a persistencia (S10).
- [x] Tablero BI con evaluación en tiempo real (S11).
- [x] Puente continuo Supabase → Kafka y adaptador de minutos vivos tolerante a 10 s.
- [x] Inferencia continua Spark MLlib con horizonte de 120 s y anticipación real $\approx 43.7\text{ s}$.
- [x] Evento empresarial derivado con umbral de control y cooldown de 300 s.
- [x] Prueba de resiliencia ante interrupciones e idempotencia en SQLite.
- [x] Guion de sustentación técnica individual preparado (`docs/guion-s12.md`).
- [ ] Ensayar la exposición oral individual de 3 minutos y defensa de preguntas.
- [ ] Integración final del equipo y publicación en GitHub cuando el usuario lo disponga.

Referencias: [evaluación S12](https://262bigdata.github.io/lambda26/sesiones/S12_Evaluacion_Unidad_2/) y [estructura oficial del producto U2](https://262bigdata.github.io/lambda26/proyecto-sello/u2-producto/). Los resultados y limitaciones corresponden estrictamente al proyecto local comprobado.
