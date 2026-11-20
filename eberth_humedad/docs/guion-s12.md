# Sustentación de humedad en AireIQ (Evaluación S12 - Producto U2)

Responsable: Eberth Emmanuel Rosales Castillo. Este guion prepara la defensa individual del Producto U2 (Sesiones S6 a S12). Toda la evidencia técnica proviene de ejecuciones locales verificadas con manifiestos reproducibles. No se atribuyen aprobaciones docentes previas ni componentes no implementados.

---

## 1. Presentación técnica individual (8 minutos)

| Tiempo | Foco temático | Explicación técnica y evidencia a proyectar |
|---|---|---|
| **0:00–1:00** | **Problema y pregunta predictiva U2** | En U1 resolvimos estimación contemporánea. En U2 el reto es el tiempo real: *¿Cuál será la humedad media del siguiente minuto en el dispositivo observado?* No basta con esperar a que termine el minuto; se requiere anticipación útil para tomar decisiones operativas sin perder rigor estadístico. |
| **1:00–2:15** | **Arquitectura de eventos Lambda y Kafka (S6/S7)** | Proyectar el flujo: ESP32/BME680 → Supabase → puente seguro de lectura → topic `aireiq-mediciones-supabase-v1` (key=`dispositivo`). Explicar el desacoplamiento: el sensor y el broker no dependen de la disponibilidad simultánea de Spark. Mostrar en Kafka UI (`http://localhost:48085`) las particiones y contratos v1 de telemetría e intervenciones (`aireiq-intervenciones-v1`). |
| **2:15–3:30** | **Procesamiento de streaming, ventanas y watermark (S8/S12)** | Explicar el manejo de eventos fuera de orden y tardíos mediante Watermark (2 minutos). Mostrar la prueba deslizante de S12 (`demo_s12_ventanas_visible.ps1`): ventanas de 5 min desplazadas cada 1 min, deduplicación de eventos duplicados, descarte de eventos posteriores al watermark y recuperación determinista mediante checkpoints sin duplicación de resultados. |
| **3:30–4:45** | **El desafío de disponibilidad: horizonte 120 s (S10)** | Explicar por qué fallaba el horizonte a 60 segundos: el minuto $t$ se cierra en $t+70\text{ s}$ por el búfer de gracia del sensor (10 s), haciendo matemáticamente imposible anticipar $t+60\text{ s}$. Solución de ingeniería: entrenar y validar el horizonte a 120 segundos ($t+2$), garantizando un margen de anticipación real comprobado de $\approx 43.7\text{ s}$. Explicar por qué persistencia fue el modelo ganador frente a regresión lineal y Random Forest en la validación cronológica. |
| **4:45–6:00** | **Inferencia continua en vivo y evaluación oportuna (S11/S12)** | Mostrar el servicio `live-forecast` con Spark Structured Streaming + MLlib (`services/streaming/live_forecast.py`). Explicar la regla de oportunidad: solo se evalúan predicciones emitidas antes del inicio del minuto objetivo. El emparejamiento con la observación real se almacena en SQLite (`evaluation.sqlite`) al llegar el agregado real. Mostrar métricas vivas: 18 observaciones, 9 oportunas, 5 pares evaluados con RMSE $\approx 1.74$. |
| **6:00–7:00** | **Evento empresarial derivado y cooldown** | Cierre de la lógica funcional: la predicción no es un dato aislado; si la humedad predicha excede los límites de control estadístico ($p_{95} = 42.0\%$ o $p_5 = 30.0\%$ de U1), se emite automáticamente una alerta `ventilacion_preventiva_sugerida` a `aireiq-intervenciones-v1`. Explicar el mecanismo de *cooldown* (300 s): evita inundar el broker con alertas redundantes en minutos sucesivos. El consumidor auditor valida y confirma en `aireiq-intervenciones-auditadas-v1`. |
| **7:00–8:00** | **Observabilidad, costos y resiliencia ante caídas** | Mostrar el tablero de Grafana [AireIQ · Humedad y predicción real](http://localhost:43000/d/aireiq-vivo) y el tablero operativo S9. Métricas en Prometheus vía Pushgateway. Demostrar la resiliencia e idempotencia: ante un reinicio o caída de Spark, las claves primarias compuestas `(device, minute)` en SQLite y los offsets de checkpoint evitan duplicaciones y garantizan consistencia exacta. |

---

## 2. Demostración técnica práctica (5 minutos)

Ensayar en orden. Abrir PowerShell en la raíz del proyecto.

### Paso 1: Estado de los contenedores e ingesta continua (1 min)
Ejecutar:
```powershell
docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml -f compose.supabase.yaml -f compose.live.yaml ps
```
**Qué señalar:**
- `aireiq-supabase-bridge-1`: leyendo lecturas reales desde Supabase.
- `aireiq-live-minutes-1`: agregando minutos con tolerancia de 10 s.
- `aireiq-live-forecast-1`: Spark Structured Streaming haciendo inferencia en vivo.
- `aireiq-eventos-1`: consumidor auditor de eventos empresariales.
- `aireiq-kafka-1`, `aireiq-prometheus-1`, `aireiq-grafana-1`: infraestructura activa y saludable.

### Paso 2: Tablero en vivo de Grafana y anticipación real (1.5 min)
Abrir en el navegador: [http://localhost:43000/d/aireiq-vivo](http://localhost:43000/d/aireiq-vivo) (credenciales de `.env`).
**Qué señalar:**
- Panel **Margen antes del inicio objetivo**: muestra $\approx 43.7\text{ s}$ de anticipación real.
- Panel **Humedad observada vs Última predicción oportuna**: valores actuales de la serie en vivo.
- Panel **RMSE vivo** y **Pares evaluados**: 5 pares contrastados sin inventar datos ni usar relleno artificial.
- Paneles **Intervenciones de negocio emitidas** y **Alertas suprimidas por cooldown**.

### Paso 3: Auditoría y eventos derivados en Kafka (1.5 min)
Ejecutar la verificación del evento empresarial derivado:
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verificar_evento_pronostico_visible.ps1
```
**Qué señalar en la salida:**
- Evento emitido a `aireiq-intervenciones-v1` con `accion: ventilacion_preventiva_sugerida` y `origen: pronostico_automatico`.
- Consumo inmediato y validación por `aireiq-eventos-1`.
- Confirmación de auditoría en `aireiq-intervenciones-auditadas-v1` con partición y offset asignado.
- Abrir Kafka UI en [http://localhost:48085](http://localhost:48085) y mostrar el mensaje en el topic.

### Paso 4: Evidencia de resiliencia e idempotencia (1 min)
Ejecutar:
```powershell
docker exec aireiq-live-forecast-1 python /app/verificar_resiliencia.py
```
**Qué señalar:**
- Cero duplicados en observaciones, predicciones e intervenciones (`idempotent: true`).
- Coincidencia exacta de conteo total y conteo único gracias a la clave primaria `(device, minute)`.
- 18 archivos de commit y offset en el checkpoint de Spark garantizando recuperación determinista.

---

## 3. Preguntas clave para la defensa individual (5 minutos)

### 1. ¿Por qué se utilizó un horizonte predictivo de 120 segundos en lugar de 60 segundos?
> **Respuesta:** En un entorno de streaming real con sensores físicos, el minuto $t$ termina en $t+60\text{ s}$, pero debido a la latencia de transmisión y a la ventana de tolerancia de 10 s para capturar lecturas rezagadas, el agregado del minuto solo está cerrado y disponible en el bus a las $t+70\text{ s}$. Si intentáramos predecir el minuto $t+1$ (que comienza a $t+60\text{ s}$), la predicción se emitiría 10 segundos después de que el minuto objetivo ya comenzó, careciendo de utilidad predictiva. Con el horizonte $t+2$ ($t+120\text{ s}$), la predicción emitida en $t+76\text{ s}$ tiene una anticipación real de $\approx 43.7\text{ s}$ antes de que inicie el minuto objetivo, cumpliendo con la definición estricta de pronóstico oportuno.

### 2. ¿Por qué persistencia superó a los modelos de Machine Learning (Random Forest y Regresión Lineal)?
> **Respuesta:** La humedad relativa en ambientes cerrados exhibe una altísima autocorrelación temporal a escala de 1 a 2 minutos. En validación cronológica estricta sobre 802 pares, la persistencia (usar el valor del minuto actual como estimador de $t+120\text{ s}$) obtuvo un RMSE de 0.5638 vs 0.6244 de Regresión Lineal y 3.2603 de Random Forest. En lugar de forzar un modelo complejo con sobreajuste, aplicamos el principio de rigor científico en Big Data: la referencia simple es el modelo de producción mientras ningún estimador demuestre empíricamente superarlo en datos no vistos.

### 3. ¿Cómo funciona el Watermark en Spark Structured Streaming y por qué no es infinito?
> **Respuesta:** El watermark define el umbral temporal máximo que el motor esperará por datos rezagados antes de descartarlos y liberar memoria de estado. En S8 y S12 usamos un watermark de 2 minutos sobre `event_time`. Si el watermark fuera infinito, el estado en memoria crecería indefinidamente provocando un OutOfMemory error. Si fuera cero, cualquier fluctuación de red descartaría lecturas válidas. Con 2 minutos, Spark mantiene en memoria solo las ventanas activas y purga de forma segura las ventanas cerradas.

### 4. ¿Cómo se garantiza la idempotencia ante una caída de los servicios?
> **Respuesta:** La idempotencia se garantiza en dos capas:
> 1. **En Spark Streaming:** Mediante checkpoints persistentes (`/evidence/live_forecast/checkpoint/`) que almacenan los offsets procesados en Kafka de forma transaccional. Al reiniciar el contenedor, Spark retoma exactamente en el offset no comprometido sin reprocesar desde el inicio.
> 2. **En la base de datos de evaluación SQLite:** Mediante una clave primaria compuesta `PRIMARY KEY(device, minute)` en las tablas `observations`, `predictions` e `interventions`. Si un microbatch se reejecuta, la sentencia `INSERT OR IGNORE` descarta la fila repetida, garantizando que el recuento de eventos, el RMSE y las intervenciones no se dupliquen.

### 5. ¿Qué diferencia hay entre la clave del mensaje en Kafka y la partición?
> **Respuesta:** La clave (en nuestro caso, el identificador del dispositivo `ESP32_AireIQ`) pasa por una función de hash (`murmur2` por defecto en Kafka) que asigna determinísticamente todos los eventos de ese dispositivo a una misma partición. Esto garantiza que las mediciones de un sensor lleguen estrictamente en orden secuencial a los consumidores, permitiendo calcular agregaciones y detectar deltas sin colisiones entre particiones concurrentes.

### 6. ¿Qué función cumple el mecanismo de *cooldown* en los eventos empresariales derivados?
> **Respuesta:** Si la humedad relativa supera el umbral de control (por ejemplo, sube al 48% superando el $p_{95}$ del 42%), el modelo predecirá alta humedad en cada minuto continuo mientras la condición persista. Sin un mecanismo de enfriamiento (*cooldown* de 300 s / 5 minutos), el sistema emitiría 60 alertas por hora saturando el topic `aireiq-intervenciones-v1` y los sistemas de notificación de los operadores. El cooldown registra la supresión de la alerta para mantener auditoría, pero no emite un nuevo evento Kafka hasta que expire el periodo de espera.

### 7. ¿Por qué el RMSE en vivo ($\approx 1.74$) es mayor que el RMSE histórico de prueba ($0.33$)?
> **Respuesta:** El conjunto de prueba histórico Gold utilizó agregados cerrados con todas las lecturas completas del CSV de Supabase tras semanas de consolidación. En el flujo en vivo continuo, el adaptador cierra el minuto con una ventana de tolerancia estricta de 10 segundos para no demorar la inferencia; por tanto, algunos minutos vivos se calculan con 1 a 3 lecturas en lugar de las 4 lecturas esperadas. Además, el sensor físico está sujeto a condiciones ambientales dinámicas del momento. La evaluación en vivo refleja la realidad operativa de la captura en tiempo real.

### 8. ¿Qué diferencia hay entre métricas operativas y métricas analíticas de negocio en la arquitectura?
> **Respuesta:**
> - **Métricas operativas:** Describen la salud y el rendimiento del pipeline de datos (lag de los grupos de Kafka, offsets consumidos, tasa de eventos por segundo, memoria de contenedores, tiempo de ejecución de microbatches).
> - **Métricas analíticas / BI:** Describen la calidad de la predicción y el fenómeno físico observado (humedad observada, predicción a 120 s, RMSE acumulado, MAE, margen de anticipación real en segundos e intervenciones preventivas generadas). Ambas convergen en Prometheus y se visualizan en tableros separados de Grafana (`aireiq-vivo` vs tableros operativos de S9).

### 9. ¿Por qué se utilizó una base de datos SQLite montada en volumen para la evaluación viva en lugar de almacenar todo en Kafka?
> **Respuesta:** Kafka es un log de eventos distribuido optimizado para flujo secuencial, no para búsquedas ni uniones (*joins*) relacionales complejas entre eventos asíncronos. Para calcular el error de predicción necesitamos unir la predicción del minuto $t$ (emitida en $t$) con la observación real del minuto $t+120\text{ s}$ (que llega 130 segundos después). SQLite permite ejecutar un `JOIN` determinista con índices primarios por dispositivo y timestamp, calcular métricas agregadas (RMSE, MAE) en microsegundos y persistir la auditoría de forma duradera en el volumen montado.

### 10. ¿Cuáles son los límites de la solución actual y qué se requeriría para llevarla a producción a gran escala?
> **Respuesta:**
> 1. **Alta disponibilidad:** Actualmente se ejecuta en un único broker Kafka y un maestro local Spark (`local[2]`). Producción exigiría un clúster Kafka multi-broker (factor de replicación $\ge 3$) y Spark sobre Kubernetes/YARN.
> 2. **Multi-dispositivo:** El sink actual itera en el driver; para miles de sensores se implementaría un sink distribuido hacia una base de datos columnar (como ClickHouse o Cassandra).
> 3. **Puente HTTP:** El puente hacia Supabase usa sondeo (*polling* cada 15 s); en un despliegue de alta frecuencia se sustituiría por Webhooks o Change Data Capture (CDC) vía Debezium hacia Kafka.

---

## 4. Matriz de evidencias locales comprobables

| Componente | Archivo / Recurso | Verificación técnica |
|---|---|---|
| Contrato e ingesta S6 | `services/eventos/app.py`, `outputs/s06/` | Manifiesto S6 `status=passed`, auditoría en `aireiq-intervenciones-auditadas-v1` |
| Ingesta IoT S7 | `services/eventos/iot.py`, `outputs/s07/` | Partición determinista por dispositivo `ESP32_AireIQ` |
| Ventanas deslizantes S8/S12 | `scripts/demo_s12_ventanas_visible.ps1`, `outputs/s08_deslizante/` | Manifiesto deslizante `ba20ea57...` con `status=passed`, media 50, descarte por watermark |
| Observabilidad S9 | `infra/prometheus/`, `infra/grafana/`, `outputs/s09/` | Prometheus (`localhost:49090`), Grafana (`localhost:43000`), exporter de Kafka |
| Modelo 120 s S10 | `outputs/s10_h120/20261120T140558756871Z/manifest.json` | Persistencia ganadora (RMSE 0.5638 vs 3.2603 RF) |
| Inferencia viva S11/S12 | `services/streaming/live_forecast.py`, `outputs/live_forecast/` | Verificación API datasource Grafana `outputs/live_forecast/verification.json` (`status=passed`) |
| Evento derivado + cooldown | `scripts/verificar_evento_pronostico.py`, `outputs/live_forecast/manifest_evento_empresarial.json` | Manifiesto `status=passed`, validación y auditoría en Kafka |
| Idempotencia y resiliencia | `scripts/verificar_resiliencia.py`, `outputs/live_forecast/manifest_resiliencia.json` | Claves primarias compuestas sin duplicados, 18 commits en checkpoint de Spark |
