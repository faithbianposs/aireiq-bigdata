# S9 Observabilidad de AireIQ

Prometheus recolecta métricas de Kafka y del progreso real de Spark; Grafana consulta Prometheus. El tablero técnico no constituye todavía la visualización predictiva de U2 ni la integración BI de S11.

## Componentes

| Servicio | Función | Acceso local |
|---|---|---|
| Kafka Exporter 1.10.0 | Offsets, grupos y lag Kafka | http://localhost:49308/metrics |
| Exporter AireIQ | Traduce progreso real de Spark a métricas Prometheus | Interno: telemetry:9108 |
| Prometheus 3.14.0 | Recolecta cada cinco segundos y evalúa reglas | http://localhost:49090 |
| Grafana 11.4.0 | Tablero provisionado automáticamente | http://localhost:43000/d/aireiq-operacion |

Las versiones Prometheus/Grafana/Kafka Exporter corresponden al Compose del docente consultado. Se usa scraping directo para el proceso persistente; Pushgateway y los KPIs predictivos quedan para S11.

El usuario Grafana está en `GRAFANA_ADMIN_USER` y la contraseña local en `GRAFANA_ADMIN_PASSWORD`, dentro de `.env` excluido de Git. No se imprimen credenciales en los registros. La configuración inicial de Grafana aplica al crear su base; cambiar el valor del archivo después no implica que cambie una contraseña ya guardada por Grafana.

## Interpretación de los paneles

| Panel | Significado y límite |
|---|---|
| Servicios accesibles | `up` confirma que Prometheus puede consultar cada endpoint, no que todo el negocio funcione |
| Spark activo y heartbeat | La consulta indica actividad y actualiza un archivo de estado; más de 30 segundos sin actualización se considera obsoleto |
| Pendientes de Spark | Diferencia por partición entre offset final Kafka y offset procesado por Spark, sumada; requiere ambas series presentes |
| Eventos por segundo | Tasa de aumento de offsets Kafka; puede ser cero con el productor detenido |
| Duración del último lote | Tiempo de ejecución de un microbatch, no latencia desde captura hasta presentación |
| Rendimiento del último lote | Filas procesadas por segundo de ese lote; revisar su antigüedad antes de interpretarlo como capacidad actual |
| Memoria del estado | Memoria que reportan los operadores de agregación/deduplicación; no es la RAM total del contenedor |
| Tardíos descartados | Descartes por watermark del último lote con datos; no es contador acumulado |
| Lag del auditor empresarial | Avance confirmado del consumidor Python S6; distinto del avance Spark administrado en checkpoint |

El archivo de estado Spark se reemplaza atómicamente cada cinco segundos y se conserva el último lote con datos para distinguir inactividad de pérdida de métricas. Datos faltantes no se rellenan con valores inventados. La retención Prometheus se limita a siete días; los volúmenes de Prometheus y Grafana persisten entre recreaciones normales.

## Alertas

Se cargan tres reglas en Prometheus:

1. Exporter inaccesible durante 30 segundos.
2. Spark detenido o sin heartbeat reciente durante 30 segundos.
3. Más de 1000 eventos pendientes de Spark durante un minuto.

Los umbrales son iniciales para el laboratorio, no acuerdos de servicio medidos. La alerta de heartbeat tiene una prueba sintética con `promtool`; no implica haber detenido el sistema real ni haber probado notificaciones externas. No hay Alertmanager ni envío de mensajes configurado.

## Demostración visible

```powershell
powershell -NoProfile -NoExit -ExecutionPolicy Bypass -File scripts/demo_s09_visible.ps1
```

El script reconstruye Spark para publicar su estado, recupera el checkpoint existente, inicia la observabilidad y verifica configuración y reglas. Después reproduce 30 lecturas antiguas para mostrar actividad. Como el watermark ya avanzó en S8, esas lecturas pueden descartarse del agregado: el aumento de offsets y los descartes son compatibles. No se presentan como nuevas mediciones del sensor.

La comprobación usa APIs de Prometheus y Grafana: targets accesibles, consulta real a través del datasource Grafana, tablero provisionado, Spark activo y backlog cero después de procesar. Los manifiestos están en `outputs/s09/<run_id>/manifest.json`. La revisión visual de todos los paneles es independiente de esta comprobación funcional por API.

Los comandos visibles y salidas están en `outputs/s09/`. `resources.log` es una instantánea real de CPU/RAM Docker, no un promedio ni una prueba de carga sostenida.

## Costos y criterios de escalado

La estimación se encuentra en [costos S9](s09-costos.md). Debe actualizarse con tamaño real de eventos, horas de ejecución, retención y tarifas de un proveedor si se elige uno. No se ha contratado infraestructura externa.

Fuentes: [observabilidad del docente](https://github.com/262bigdata/lambda26/blob/main/obs/compose.yml) y [Kafka del docente](https://github.com/262bigdata/lambda26/blob/main/kafka/compose.yml).
