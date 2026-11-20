# S8 Procesamiento de humedad en streaming

Spark Structured Streaming consume `aireiq-mediciones-v1` directamente de Kafka. Se ejecuta en un contenedor propio que comparte la imagen base de U1, con PySpark 4.2.0 y el conector `spark-sql-kafka-0-10_2.13:4.2.0`. Jupyter queda disponible para explorar y explicar.

## Flujo

```text
Kafka → JSON con esquema explícito → validación → watermark
      → deduplicación → ventanas fijas de un minuto → Parquet
```

La validación excluye identificadores/fechas esenciales ausentes, claves incorrectas, valores fuera de los límites y fallos de gas. Las exclusiones de esta consulta no se escriben a un nuevo topic de rechazo; S7 mantiene la auditoría de calidad del replay probado. No confundir filas filtradas con datos descartados por watermark.

Se deduplica por `(replay_run_id, event_id)` dentro del horizonte del watermark. Una corrida nueva de reproducción constituye una demostración separada y no se mezcla con otra al agregar. Para una fuente productiva futura deberá definirse la identidad sin depender de un ID de replay.

Las ventanas son fijas, de un minuto, basadas en `event_time`, con conteo, promedio, mínimo y máximo de humedad por dispositivo y corrida. Esta implementación no incluye aún ventanas deslizantes. Se mantiene UTC en el procesamiento; para presentar los resultados se puede convertir a Lima.

## Watermark de dos minutos

El watermark se deriva del mayor tiempo de evento observado menos dos minutos. No avanza simplemente por esperar dos minutos de reloj. Tolera desorden acotado y permite retirar estado antiguo. La garantía de tolerancia se interpreta con el progreso real de cada operador; un evento más antiguo que el horizonte puede descartarse.

La salida Parquet usa modo append: una ventana se publica cuando el watermark permite finalizarla. Las últimas ventanas permanecen en estado hasta recibir eventos con tiempos posteriores; no se fuerza su cierre inventando mediciones del sensor. Las ventanas fijas suman lecturas válidas; no implican cobertura continua cuando hay huecos.

En un replay histórico mezclado con eventos más recientes, el watermark puede haber avanzado y rechazar lecturas antiguas. Para una demostración aislada utilizar un checkpoint y salida nuevos; nunca borrar un checkpoint productivo para intentar ocultar pérdidas. El código de prueba usa un topic de control independiente con origen `prueba_controlada`.

## Checkpoints y entrega

Cada consulta conserva offsets, estado y metadatos en su checkpoint persistente. Al reabrir la misma consulta con el mismo checkpoint y salida se recupera su avance; `startingOffsets=earliest` aplica a una consulta nueva, no sobrescribe un checkpoint existente.

La prueba detiene y vuelve a crear la consulta, comprueba que conserva el query ID, cambia el run ID, no duplica ventanas ya emitidas y continúa procesando nuevas entradas. No es una prueba de apagón del equipo ni de pérdida del volumen.

El sink de archivos de Spark utiliza su registro `_spark_metadata`. Los lectores deben usar la raíz de salida con `spark.read.parquet`, para respetar ese registro. Esto no convierte todo el sistema, incluidos productores y otras salidas, en exactamente una vez. La deduplicación está acotada por el watermark.

## Ejecutar en PowerShell visible

```powershell
powershell -NoProfile -NoExit -ExecutionPolicy Bypass -File scripts/demo_s08_visible.ps1
```

La ventana muestra cuatro pasos: construir, probar con datos controlados, procesar el histórico disponible mediante `availableNow` y dejar activo el consumidor continuo. `availableNow` sigue siendo Structured Streaming con microbatches y checkpoint, pero termina al procesar lo disponible; el servicio `live` permanece esperando nuevos datos.

| Evidencia | Ubicación |
|---|---|
| Prueba controlada y progreso de operadores | `outputs/s08/<run_id>/` |
| Integración con el histórico real en Kafka | `outputs/s08_historico/<run_id>/` |
| Salida y checkpoint del consumidor continuo | `outputs/s08_live/` |
| Registro visible de ejecución | `outputs/s08/demo_visible.log` |

Un manifiesto `status=passed` confirma las aserciones de esa ejecución. Revisar `eventTime.watermark`, `numRowsDroppedByWatermark`, `stateOperators`, `numInputRows` y offsets de la fuente en los registros de progreso. Los offsets de Spark se administran en el checkpoint; Kafka UI no representa necesariamente su avance como el grupo consumidor Python de S7.

## Cómo explicarlo

«Kafka conserva los eventos y Spark calcula resultados por tiempo de captura. El watermark limita cuánto desorden temporal admitimos y cuánto estado conservamos. El checkpoint permite recuperar el avance. Probamos un duplicado, una lectura inválida, una tardía y el reinicio de la consulta antes de aplicar el flujo al histórico real».

No se ha conectado Supabase en vivo, entrenado el pronóstico U2 ni integrado Grafana en esta etapa. Las capturas no se incluyen y GitHub continúa pospuesto.

Fuente técnica: [conector oficial Spark 4.2.0 para Kafka](https://spark.apache.org/docs/4.2.0/streaming/structured-streaming-kafka-integration.html).
