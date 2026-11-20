# S6 Eventos empresariales de AireIQ

Esta etapa adapta el patrón productor/consumidor al registro de intervenciones manuales del ambiente. Registrar una ventilación o mantenimiento es un evento de negocio de baja frecuencia; no es una lectura del sensor ni una orden enviada a un actuador. La demostración usa `origen=simulacion`.

## Arquitectura implementada

```text
Productor Python de consola → aireiq-intervenciones-v1
                           → consumidor auditor
                           → aireiq-intervenciones-auditadas-v1
                           → aireiq-intervenciones-invalidas-v1 si el contrato falla
```

Se utilizan Apache Kafka 4.3.1 y Kafka UI 1.5.0, versiones del Compose actual del docente. Cada topic tiene tres particiones y un factor de replicación de uno. El único broker de laboratorio no ofrece alta disponibilidad. El volumen `kafka-data` conserva datos entre recreaciones del contenedor; no usar `down -v` si se desean conservar.

La ruta aplicada aquí usa un productor Python ejecutado por comando y un consumidor persistente. El ejercicio específico de S6 del docente utiliza dos microservicios Spring Boot de órdenes/pagos. Esta adaptación NO acredita ese ejercicio Spring ni el informe de tarea; cubre el flujo empresarial propio para el proyecto. La evaluación U2 exige eventos empresariales e IoT, por lo que S7 debe añadir la telemetría.

## Contrato de entrada versión 1

Topic: `aireiq-intervenciones-v1`. Key UTF-8: `ambiente_id`. Productor: `eventos publish`. Consumidor: grupo `aireiq-auditoria-v1`.

| Campo | Tipo y regla |
|---|---|
| event_id | String UUID, identidad lógica para reconocer reenvíos |
| schema_version | Entero 1 |
| occurred_at | Fecha ISO 8601 con zona horaria |
| ambiente_id | String no vacío; coincide con la key |
| tipo | `intervencion.registrada` |
| accion | `ventilacion_manual_registrada` o `mantenimiento_registrado` |
| origen | `simulacion` u `operador` |

```json
{
  "event_id": "664983ce-68b9-4f66-ad2d-93eb555c7285",
  "schema_version": 1,
  "occurred_at": "2026-11-19T22:00:00-05:00",
  "ambiente_id": "ambiente-demo",
  "tipo": "intervencion.registrada",
  "accion": "ventilacion_manual_registrada",
  "origen": "simulacion"
}
```

El ejemplo no documenta una intervención real. Los campos adicionales son tolerados en v1. Un cambio incompatible requerirá otra versión y adaptación explícita de productores/consumidores.

## Contratos de salida

La auditoría conserva `event_id`, `ambiente_id` y `origen`, añade `tipo=intervencion.auditada`, `schema_version=1`, `audited_at` y la partición/offset de origen. Confirmar recepción no confirma que haya ocurrido físicamente una ventilación.

El topic de inválidos contiene `tipo=evento.rechazado`, motivo, fecha de recepción, topic/partición/offset de origen y payload original en `raw`. Un mensaje inválido se conserva allí y el consumidor continúa. No enviar credenciales ni datos personales en estos eventos.

La salida se confirma antes de confirmar el offset de entrada. La semántica es **al menos una vez**: una caída entre esas operaciones puede duplicar la auditoría. No hay transacción Kafka ni deduplicación persistente en esta etapa. Los consumidores analíticos posteriores deberán deduplicar por identidad; no asumir exactamente una vez. El orden solo se garantiza por partición, no globalmente.

## Ejecutar y verificar

Desde PowerShell en la raíz:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/iniciar_s06.ps1
```

El script valida Compose, construye el cliente, inicia Kafka/UI/consumidor y ejecuta la prueba. Mantiene Jupyter y el trabajo U1. Kafka externo: `localhost:49092`; desde contenedores: `kafka:9092`. Kafka UI: http://localhost:48085.

Prueba: publicar un mensaje inválido seguido de tres válidos, verificar tres auditorías, el rechazo y la misma partición para la misma key. Las evidencias incluyen offsets reales en `outputs/s06/<run_id>/manifest.json`. Solo un manifiesto con `status=passed` acredita esa ejecución; no atribuir prueba a archivos de código.

Para publicar otro ejemplo desde una terminal con Docker en PATH:

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml run --rm --no-deps eventos publish --ambiente ambiente-demo --accion mantenimiento_registrado --origen simulacion
```

Para inspeccionar el grupo:

```powershell
docker compose -f compose.yaml -f compose.streaming.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group aireiq-auditoria-v1
```

## Conceptos para explicar

El broker almacena eventos. El topic agrupa un tipo de flujo. La key dirige un ambiente a una partición. El offset identifica su posición dentro de esa partición. Un grupo registra su avance y distribuye particiones entre consumidores. El lag es la diferencia entre el final del registro y el offset confirmado; no mide por sí solo la latencia temporal.

Separar productor y auditor por Kafka permite que el productor publique aunque el auditor esté detenido. Kafka conserva el evento hasta que el auditor lo lea, sujeto a retención y disponibilidad del broker. Una llamada HTTP directa requiere que ambos estén disponibles simultáneamente y necesita otra estrategia para reintentar sin perder datos.

GitHub se pospone por indicación del usuario. Las capturas de tarea están fuera de este alcance.

Fuentes: [Compose Kafka del docente](https://github.com/262bigdata/lambda26/blob/main/kafka/compose.yml) y [sesión S6](https://github.com/262bigdata/lambda26/blob/main/docs/sesiones/S06_Ingesta_Eventos_Empresariales_Kafka.md).
