# S7 Ingesta IoT de AireIQ

Esta etapa reproduce el CSV de Supabase hacia Kafka y verifica el consumo. No conecta directamente a Supabase ni demuestra un ESP32 transmitiendo en vivo. No modifica firmware ni la fuente CSV.

## Flujo y contrato

`mediciones_aire_rows.csv → productor Python → aireiq-mediciones-v1 → consumidor de verificación → manifiesto y cuarentena`.

El topic tiene tres particiones y replicación uno, en un broker local. La key UTF-8 es `dispositivo`. Un único ESP32 usa una sola partición para mantener el orden de llegada de sus mensajes; ese orden no garantiza que las fechas de captura estén ordenadas.

| Campo | Significado |
|---|---|
| schema_version | Entero 1 |
| event_id | UUID determinista de tabla, dispositivo, ID y fecha; estable entre reproducciones |
| source_id | ID original de Supabase |
| dispositivo | Identificador que también constituye la key |
| event_time | Fecha original de captura normalizada a UTC |
| ingested_at | Instante en que el productor prepara el evento para Kafka |
| temperatura_c | Temperatura en °C |
| humedad_pct | Humedad relativa en % |
| presion_hpa | Presión en hPa |
| gas_ohms | Resistencia de gas en ohmios |
| origen | `reproduccion_csv` |
| replay | Booleano true |
| replay_run_id | Identificador nuevo por corrida de demostración |

Esta versión transmite las cuatro mediciones físicas necesarias. No atribuye carácter medido a IAQ, eCO₂ o VOC experimentales. Los futuros cambios de esquema deben versionarse.

## Calidad y trazabilidad

El productor comprueba ID, dispositivo, fecha con zona horaria y números finitos. Un registro no convertible se anota en `errores_fuente.jsonl`. Las lecturas convertibles se publican incluyendo fallos del sensor, para evidenciar validación de extremo a extremo.

El consumidor valida contrato, key, humedad entre 0–100 %, temperatura entre −40–85 °C y presión entre 300–1100 hPa; gas no positivo se rechaza conforme al brief. La cuarentena local `rechazos_sensor.jsonl` conserva evento, razón, partición y offset. Los rechazos no se convierten en alertas ni se cuentan como lecturas válidas. Los límites son técnicos amplios, no umbrales sanitarios.

Los IDs estables permiten deduplicar futuros reenvíos. La repetición deliberada del CSV vuelve a publicar eventos con esos mismos IDs y otro `replay_run_id`; S8 deberá decidir explícitamente si está procesando una nueva demostración o duplicados productivos. No se afirma exactamente una vez.

## Demostración visible

En PowerShell, desde la raíz de AireIQ:

```powershell
powershell -NoProfile -NoExit -ExecutionPolicy Bypass -File scripts/demo_s07_visible.ps1
```

La ventana muestra el comando de construcción, la reproducción y la consulta del topic. La prueba completa usa `--pausa 0`: es una ráfaga acelerada, no el ritmo del sensor. Cada mensaje espera confirmación del broker. Para una demostración corta y más lenta:

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml run --rm --no-deps --entrypoint python eventos iot.py --limite 30 --pausa 1
```

El consumidor recibe asignación y registra offsets antes de publicar, para no perder la ráfaga de prueba. Identifica solo la corrida actual, verifica identidad/partición/offset y confirma su progreso al terminar. La evidencia se guarda en `outputs/s07/<run_id>/manifest.json`. Solo `status=passed` confirma esa ejecución. La prueba también valida nueve casos del contrato, incluidos los offsets abreviados `-05` y las fracciones de segundo del CSV de Supabase.

Abrir http://localhost:48085 y seleccionar el topic `aireiq-mediciones-v1` para ver los mensajes. El topic empresarial `aireiq-intervenciones-v1` sigue separado: una intervención ocurre por una acción de negocio, una lectura IoT se genera periódicamente.

## Siguiente etapa

S8 implementará Spark Structured Streaming, ventanas, watermark, checkpoints y deduplicación. La prueba S7 usa un consumidor Python y todavía no implementa esos mecanismos. Para el replay, los cálculos de latencia deberán distinguir retraso histórico de tiempo de procesamiento; restar la fecha original a la hora actual no mide la velocidad del pipeline. Los huecos del histórico tampoco equivalen a un fallo de Kafka.

GitHub permanece pospuesto. Las capturas de tarea están fuera de alcance por indicación del usuario.
