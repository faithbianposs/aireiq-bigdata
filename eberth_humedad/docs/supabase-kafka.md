# Puente de lectura Supabase → Kafka

El servicio `supabase-bridge` consulta por GET `public.mediciones_aire` mediante la API REST. No modifica la tabla ni el firmware. Publica en `aireiq-mediciones-supabase-v1` con clave de dispositivo, `origen=supabase_poll` y `replay=false`. Esto describe el origen de la extracción, no garantiza frescura: el primer recorrido también incluye las filas históricas visibles.

## Configuración

Agregar localmente a `.env` (no enviar credenciales al chat ni subirlas a Git):

```dotenv
SUPABASE_URL=https://TU_PROYECTO.supabase.co
SUPABASE_API_KEY=TU_CLAVE_CON_ACCESO_DE_LECTURA
SUPABASE_POLL_SECONDS=15
```

La clave debe permitir SELECT sobre la tabla. Una clave publicable depende de las políticas RLS; no desactivar RLS para hacer funcionar el puente. No se requiere una clave con privilegios administrativos. La implementación admite claves actuales en `apikey` y claves JWT heredadas también como bearer. Una respuesta vacía no distingue tabla vacía de filas ocultas por RLS.

Desde PowerShell en la raíz, comprobar primero el acceso sin publicar:

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml -f compose.supabase.yaml --profile supabase run --rm --no-deps supabase-bridge --check
```

Con Kafka iniciado, hacer un recorrido y observar el tópico en Kafka UI:

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml -f compose.supabase.yaml --profile supabase run --rm supabase-bridge --once
```

Después de verificar los mensajes y el acceso real, iniciar el servicio continuo:

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml -f compose.supabase.yaml --profile supabase up -d supabase-bridge
```

## Recuperación y límites

SQLite, en el volumen `supabase-state`, registra cada evento solo después del ACK de Kafka. Si el proceso cae después del ACK y antes del commit local, puede repetirse el mismo `event_id`: entrega al menos una vez, no exactamente una vez. No ejecutar varias instancias del puente sobre ese estado ni reutilizar el volumen para otro proyecto Supabase. Borrar el volumen vuelve a publicar las filas.

La fuente debe ser una tabla append-only con IDs enteros positivos. Cada recorrido pagina por ID ascendente hasta recibir una página vacía y el siguiente vuelve al principio. Esto recupera inserciones confirmadas tarde con IDs menores al máximo anterior; evita depender de que el orden de asignación sea el orden de commit. Los cambios a filas ya enviadas y los borrados no se propagan. Las filas deben seguir disponibles al menos hasta que un recorrido pueda leerlas. Una fila no convertible detiene el recorrido: requiere corregir el dato o definir cuarentena explícita.

El intervalo de 15 segundos se cuenta después de terminar cada recorrido. El escaneo completo consume consultas en proporción al tamaño de la tabla; no se presenta como CDC ni solución de alto volumen. Una evolución posible es un outbox transaccional o CDC, previa revisión de permisos y configuración.

El tópico está separado de las reproducciones existentes. El modo `supabase-history` de Spark lo procesa con `availableNow`, hasta los offsets disponibles al iniciar. El servicio S8 de fondo sigue consumiendo el tópico histórico original; no se afirma que la predicción continua esté conectada. Falta validar frescura, disponibilidad oportuna del minuto e interrupciones reales de extremo a extremo.

## Integración real verificada el 20 de noviembre de 2026

Tras habilitar el propietario la lectura pública mediante una política SELECT para `anon`, la API devolvió 500 filas en la primera página. La prueba `verify_supabase_kafka.py` leyó las primeras 20 filas reales, publicó 20 eventos y consumió los 20 IDs esperados desde Kafka. Al reabrir SQLite y repetir la misma muestra publicó cero eventos adicionales. Evidencia: `outputs/supabase/b949f5446f7e4a869e5f95b14a450120/manifest.json`.

`scripts/iniciar_supabase_visible.ps1` ejecutó esa comprobación y después inició el servicio continuo `supabase-bridge`. La prueba usa un topic aislado y no sustituye la verificación del recorrido completo. La muestra es histórica: todavía no demuestra que el ESP32 esté produciendo nuevas mediciones ahora. La clave permanece en `.env`.

## Supabase → Kafka → Spark → Parquet verificado

`scripts/demo_supabase_spark_visible.ps1` ejecutó el modo `supabase-history` y generó el manifiesto `outputs/supabase_spark/81ecb70463944cecbe735f2ae0812362/manifest.json`: 16 476 entradas Kafka y 4 430 ventanas finalizadas. Se aplicaron el filtro de calidad existente, deduplicación, ventana de un minuto y watermark de dos minutos; las ventanas aún abiertas no se fuerzan a cerrar. Los conteos de ventanas y eventos representan unidades distintas.

El script espera a que Kafka esté saludable, pausa Jupyter y Spark de fondo para reducir memoria y los reanuda al finalizar. Cada ejecución usa una carpeta/checkpoint propios y vuelve a leer el tópico; no es un consumidor continuo ni reemplaza los checkpoints existentes. El primer intento falló por indisponibilidad de Kafka durante el arranque; el segundo terminó con `status=passed`.

## Pruebas

`scripts/demo_supabase_pruebas_visible.ps1` construye el contenedor y ejecuta pruebas con fuente y productor simulados: paginación, reapertura de SQLite, inserción tardía, reintento con ID estable y rechazo de fecha inválida. El resultado está en `outputs/supabase/pruebas.log`. Estas pruebas no acreditan conectividad HTTP ni publicación real a Kafka.

Referencia: [API REST de Supabase](https://supabase.com/docs/guides/api), usada para definir la ruta de lectura y el acceso mediante claves y políticas.
