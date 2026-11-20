# Referencia AquaVigía y decisiones para AireIQ

Se revisaron el PPT de 26 diapositivas proporcionado por el usuario, el README, el documento U2 de Patricia y el productor USGS del repositorio público. La aprobación del docente fue comunicada por el usuario; no se verificó una calificación independiente. Sus números son resultados declarados por ese equipo y no evidencias de AireIQ.

## Qué aporta a nuestra entrega

- Organizar la defensa por criterio, enlazando cada afirmación con un cuaderno, mensaje, log o panel verificable.
- Separar los datos históricos reproducidos de los registros recibidos después de iniciar la observación.
- Medir retraso y tamaño de eventos para justificar watermark y recursos. El watermark de dos minutos de AireIQ está probado funcionalmente, pero aún requiere justificación con datos del flujo real.
- Relacionar una predicción con un evento empresarial y un KPI. Se integró la emisión automática de alertas a `aireiq-intervenciones-v1` basada en los umbrales p5/p95 de U1, con un mecanismo de cooldown de 300 s para evitar alertas repetidas, auditada en `aireiq-intervenciones-auditadas-v1`.
- Conservar resultados negativos: en AireIQ ganó persistencia. No sustituirlos por las mejoras declaradas para otras variables del equipo de referencia.

## Diferencias que debemos conservar

La referencia usa USGS y un horizonte de una hora; AireIQ usa BME680, Supabase y un horizonte de un minuto. No corresponde copiar su watermark de 85 minutos, sus precios ni sus métricas. El documento U2 más reciente del repositorio describe eventos con contexto y una versión 3 más amplia que varias diapositivas del PPT: los materiales son revisiones diferentes.

El firmware de AireIQ ya publica en Supabase. El puente de solo lectura respeta ese flujo; no se ha añadido MQTT ni se presenta el puente HTTP como si fuera MQTT. Los requisitos específicos del laboratorio se deben contrastar con la guía del docente.

## Evidencias propias implementadas y verificadas

1. Supabase → Kafka → Spark → Parquet (`outputs/supabase_spark/81ecb70463944cecbe735f2ae0812362/manifest.json`, status="passed").
2. Puente de lectura continua y adaptador de minutos (`outputs/live_minutes/`).
3. Disponibilidad temporal resuelta con horizonte de 120 s y anticipación real de $\approx 43.7\text{ s}$ (`outputs/live_forecast/status.json`).
4. KPI y evento empresarial preventivo derivado con umbral estadístico y cooldown de 300 s (`outputs/live_forecast/manifest_evento_empresarial.json`, status="passed").
5. Resiliencia ante caídas e idempotencia verificadas (`outputs/live_forecast/manifest_resiliencia.json`, status="passed").
6. Guion de sustentación técnica por criterios oficiales en `docs/guion-s12.md`.

Fuentes: [repositorio AquaVigía](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/tree/main), [producto U2 de Patricia](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/blob/main/docs/patricia/u2-producto.md), y PPT local `aquavigia_u2_equipo02_iauo283vunxg.pptx` aportado como contexto. No se ejecutó código del repositorio externo.
