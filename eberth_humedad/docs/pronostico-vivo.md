# Inferencia continua y evaluación de humedad

Ruta implementada: ESP32 → Supabase → puente de lectura → Kafka → adaptador de minutos → `aireiq-minutos-vivos-v1` → Spark Structured Streaming + PipelineModel → SQLite → Pushgateway → Prometheus → Grafana.

El tablero [AireIQ · Humedad y predicción real](http://localhost:43000/d/aireiq-vivo) es independiente del tablero de reproducción S11. El servicio `live-forecast` usa el experimento a 120 segundos. Su primera ejecución fija la corrida del modelo en `outputs/live_forecast/model.json`; no cambia automáticamente de modelo al aparecer otro entrenamiento.

## Tiempo y evaluación

Se registra la hora de emisión después de calcular la predicción. El inicio objetivo es exactamente `minuto + 120 segundos`. Solo se considera oportuna una emisión estrictamente anterior a ese inicio. Los minutos históricos consumidos al arrancar se registran como tardíos, sin contarlos como anticipación. El margen real se expresa en segundos y puede ser menor de un minuto por el cierre y procesamiento.

Cada predicción se evalúa contra el agregado del mismo dispositivo y minuto objetivo exacto, disponible después de la emisión. No se rellena ni se sustituye un minuto ausente. El emparejamiento se conserva en SQLite y la consulta de métricas calcula error únicamente para los pares existentes y oportunos. Los paneles de error permanecen vacíos si aún no existen pares; nunca muestran cero como sustituto de falta de evidencia.

El error vivo corresponde a agregados posiblemente incompletos por el límite de recepción del adaptador. No equivale al test histórico Gold. Persistencia fue el método seleccionado; la referencia y el modelo tienen la misma predicción. RMSE y MAE están en puntos porcentuales de humedad.

## Arranque y estado

Ejecutar `scripts/iniciar_pronostico_vivo_visible.ps1`. Construye la imagen, verifica tres pruebas de evaluación y levanta servicios necesarios. Detiene el Spark de reproducción `spark-streaming` para reducir memoria; Jupyter permanece disponible. El panel operativo S9 que consulta el checkpoint anterior no describe este nuevo servicio: usar el estado y antigüedad del tablero vivo. No interpretar esa alerta anterior como diagnóstico de este flujo.

`scripts/verificar_pronostico_vivo_visible.ps1` consulta las métricas mediante el datasource de Grafana durante un máximo de cinco minutos. Solo produce `status=passed` si observa servicio activo y reciente, predicciones oportunas y al menos un par real evaluado. `pending_live_evidence` significa que no se acreditó esa condición, no que se inventó una muestra. La revisión es por API; no acredita revisión visual.

Estado local: `outputs/live_forecast/status.json`. Evidencia de verificación: `outputs/live_forecast/verification.json`. SQLite: `outputs/live_forecast/evaluation.sqlite`. Checkpoint de Spark: `outputs/live_forecast/checkpoint/`.

## Recuperación y límites

Las claves primarias por dispositivo/minuto hacen idempotente el registro si un microbatch se repite. Checkpoint, SQLite y modelo fijado deben conservarse juntos; no borrar uno conservando los demás. El sink recorre filas en el driver, adecuado al único dispositivo actual; no acredita escalado de almacenamiento ni alta disponibilidad. Las observaciones y predicciones se conservan para auditoría y el archivo crece con el tiempo.

Pushgateway conserva valores después de una caída: revisar tanto `active` como antigüedad de actualización y del dato. Un fallo de publicación de métricas se registra en el estado y se reintenta sin detener la inferencia. El evento empresarial derivado con umbral estadístico (p95 = 42.0%, p5 = 30.0%) y cooldown (300 s) se encuentra implementado y auditado (`outputs/live_forecast/manifest_evento_empresarial.json`, status="passed"). Las pruebas de resiliencia ante caídas e idempotencia determinista están verificadas (`outputs/live_forecast/manifest_resiliencia.json`, status="passed"). La preparación de la sustentación técnica se encuentra en `docs/guion-s12.md`.
