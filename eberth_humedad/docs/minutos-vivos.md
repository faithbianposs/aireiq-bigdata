# Minutos nuevos y disponibilidad real

El adaptador `live-minutes` consume `aireiq-mediciones-supabase-v1` y publica agregados en `aireiq-minutos-vivos-v1`. Es una etapa pequeña de preparación en Python/SQLite, no reemplaza las ventanas distribuidas de Spark verificadas en S8. Conserva promedios de humedad, temperatura y presión por dispositivo y minuto para alimentar el contrato de características del modelo.

## Regla de cierre

Se admiten lecturas hasta diez segundos después del final de su minuto, según el reloj del contenedor. Una lectura que llega en o después de ese límite se cuenta como `past_deadline`; no corrige un minuto ya emitido. El histórico inicial no se convierte en pronóstico actual. Gas no positivo, rangos inválidos, claves inconsistentes y eventos de origen incorrecto se excluyen; relojes más de cinco segundos en el futuro se cuentan aparte.

Los diez segundos son una política inicial de prueba, no un umbral calibrado con una muestra representativa. El puente consulta cada quince segundos más el tiempo de recorrer la tabla: pueden faltar lecturas en el agregado, incluso con el sensor conectado. El evento declara `incomplete_possible=true` y registra conteo, hora de disponibilidad y demora de cierre. No se afirma equivalencia exacta con Gold, que usa el histórico completo.

## Horizonte predictivo resuelto (120 segundos)

Si el minuto t se cierra en t+60 s y se espera la tolerancia de 10 s, la media de t está disponible aproximadamente en t+70 s. El minuto objetivo t+1 ya comenzó en t+60 s; aplicar un modelo con horizonte t+1 llegaría tarde y carecería de anticipación real.

Para resolver este desafío de ingeniería, se definió, entrenó y validó el horizonte a 120 segundos (t+2) en `docs/pronostico-120s.md` (`outputs/s10_h120/`). Al emitirse la predicción en t+76 s, se dispone de un margen de anticipación real de ≈43.7 segundos antes de que inicie el minuto t+120 s. Este flujo está integrado de forma continua en `services/streaming/live_forecast.py` y se visualiza en el tablero Grafana `aireiq-vivo`.

## Persistencia y límites

SQLite conserva lecturas recientes y agregados publicados en `minutes-state`. Las lecturas se confirman localmente antes de avanzar offsets del grupo `aireiq-live-minutes-v1`; los IDs únicos evitan duplicar lecturas tras reiniciar. Los agregados se registran después del ACK de Kafka. Un fallo entre ACK y registro puede repetir el agregado con el mismo ID; el consumidor debe deduplicarlo. Solo se admite una instancia del adaptador. No borrar su estado conservando los offsets del grupo: se perderían minutos pendientes.

Tras una caída, un minuto pendiente puede publicarse con mayor demora; `available_at` y `closure_delay_seconds` lo exponen. Los consumidores predictivos deben rechazar resultados que lleguen después del horizonte útil. Las lecturas de minutos publicados se conservan un día; los agregados permanecen en el registro para auditoría, con crecimiento proporcional al número de minutos.

## Ejecución y verificaciones

Ejecutar `scripts/iniciar_minutos_visible.ps1` en PowerShell. El script construye la imagen, ejecuta tres pruebas y arranca el servicio. Las pruebas verifican promedio, duplicados, frontera exacta del cierre, reapertura del estado, reintento tras fallo de ACK, calidad y reloj futuro. Pasaron el 20 de noviembre de 2026; el log está en `outputs/live_minutes/demo_visible.log`.

El estado operativo se escribe en `outputs/live_minutes/status.json`. Al arrancar, se observaron registros históricos descartados y cero minutos publicados; eso no acredita todavía una publicación real de minutos nuevos. No se generan datos ficticios para ocultar la ausencia del sensor.
