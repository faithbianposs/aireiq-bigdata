# Costos y escalado del laboratorio AireIQ

El sistema se ejecuta en Docker Desktop sobre el equipo local. No se ha desplegado ni contratado una nube. El costo incremental local depende de consumo eléctrico, horas de ejecución y almacenamiento; CPU y RAM reservadas no son cargos monetarios por sí mismas.

## Qué está medido y qué es una hipótesis

- La ejecución S7 transportó 15 690 mensajes. Las frecuencias históricas del brief fueron aproximadamente 16 y 61 segundos entre lecturas, con interrupciones; no representan disponibilidad continua.
- `outputs/s09/resources.log` registra una instantánea real de uso CPU/RAM. No mide energía ni permite establecer capacidad máxima.
- Para dimensionar se usa un escenario hipotético continuo de un mensaje cada 16 segundos y 600 bytes por evento. Los 600 bytes son una hipótesis de planificación, no un tamaño medido.

## Almacenamiento Kafka estimado

Fórmula: `dispositivos × 86400 / intervalo_segundos × bytes_evento × dias_retencion × replicas × factor_sobrecarga`.

| Escenario hipotético | Mensajes diarios | Payload diario | Siete días, factor 1,3 |
|---|---:|---:|---:|
| 1 sensor, 16 s, réplica 1 | 5 400 | 3,24 MB | 29,484 MB |
| 100 sensores, 16 s, réplica 1 | 540 000 | 324 MB | 2,9484 GB |
| 100 sensores, 16 s, réplicas 3 | 540 000 | 324 MB de origen | 8,8452 GB |

Se usan unidades decimales. El factor 1,3 es ilustrativo: la sobrecarga real depende de segmentos, índices, compresión y configuración. No incluye Parquet, checkpoints, imágenes Docker, Prometheus, copias ni réplicas adicionales. Los topics de prueba también consumen espacio y no se borran automáticamente. No debe interpretarse esta tabla como retención ya configurada por topic o capacidad garantizada.

## Energía y presupuesto

`costo_electrico = potencia_incremental_kW × horas_diarias × dias × tarifa_por_kWh`.

Ejemplo de consumo, sin asignar una tarifa: 40 W adicionales durante ocho horas al día y 30 días equivalen a 9,6 kWh. A 24 horas diarias serían 28,8 kWh. Los 40 W son una suposición, no una medición del equipo. Multiplicar por la tarifa real del recibo para obtener moneda local.

Para nube, completar antes de decidir: cómputo/hora × horas + almacenamiento GB/mes + transferencias + monitoreo y copias. Registrar proveedor, región, fecha, moneda y fuente de cada tarifa. No se incluye un precio de nube sin cotización.

## Memoria y riesgos

Los límites de Compose suman aproximadamente 11,875 GiB entre Jupyter, Kafka, UI, eventos, Spark Streaming, exporter Kafka, telemetry, Prometheus y Grafana. Es un techo configurado, no uso actual ni reserva garantizada. El equipo tiene aproximadamente 16 GB físicos y Windows/Docker también consumen memoria: mantener varios kernels Spark abiertos puede causar presión de memoria. Detener kernels que el usuario ya no necesite antes de una prueba de carga, sin cerrar trabajo activo automáticamente.

## Cuándo escalar

1. Si el backlog aumenta durante varios minutos, comparar tasa de entrada con procesamiento y duración de microbatch; identificar el cuello de botella antes de aumentar recursos.
2. Si la memoria de estado crece de forma sostenida, revisar cardinalidad, watermark y ventanas. Aumentar memoria sin revisar el estado solo posterga el problema.
3. Más particiones permiten paralelismo para múltiples claves; un único dispositivo con una key permanece en una partición. No alterar la key sin considerar el orden requerido.
4. Replicación tres exige al menos tres brokers y aumenta almacenamiento y tráfico. Un solo broker local no tolera su propia caída.
5. Validar cualquier cambio con carga controlada, métricas y presupuesto actualizado. El ensayo de 30 eventos de S9 no acredita el escenario de 100 sensores.
