# Producto U1 de humedad en AireIQ

Responsable académico: Eberth Emmanuel Rosales Castillo, equipo Novoa. Ejecución integrada verificada el 19/11/2026 22:07:28 UTC-05. La estimación de humedad funciona técnicamente, pero el modelo seleccionado no superó la referencia de media en prueba. No se recomienda como mejora predictiva en producción con esta evidencia.

## 1 Arquitectura Big Data seleccionada

Lambda permite analizar el histórico y añadir procesamiento de eventos en U2. La ruta implementada en esta entrega es:

```text
ESP32 y BME680 → Supabase → exportación CSV → PySpark
                                         → Bronze original
                                         → Silver por lectura válida
                                         → Gold por minuto
                                         → MLlib y resultados U1
Ruta prevista U2: Supabase → puente → Kafka → Spark Streaming → Grafana
```

La exportación CSV es el punto de entrada probado; no se ha probado aquí una conexión directa a Supabase. Se conserva el firmware existente. Docker contiene Jupyter/PySpark, con dos hilos locales (`local[2]`), sin despliegue multinodo. Los datos pertenecen a un único sensor y contienen interrupciones.

Pregunta descriptiva: ¿cómo varía la humedad por hora y día y qué porcentaje de lecturas válidas queda fuera del rango histórico p5–p95?

Pregunta predictiva: ¿qué tan precisamente se estima la humedad relativa usando temperatura, presión y hora del mismo instante? Este objetivo difiere del pronóstico a un minuto de U2.

## 2 Transformaciones distribuidas con PySpark

El [cuaderno integrado](../notebooks/05_producto_u1_humedad.ipynb) contiene lectura con esquema explícito, `withColumn`, `filter`, `groupBy().agg()`, RDD y planes `explain(True)`. La [copia ejecutada](../outputs/05_producto_u1_humedad_ejecutado.ipynb) conserva las salidas. Transformar construye el plan; acciones como `count`, `show` y escritura solicitan los resultados.

Se convierten tipos de forma controlada y se preserva el texto original en Bronze y en las filas rechazadas. Se agrega por minuto para reducir el efecto del cambio de frecuencia de captura. No se interpolan huecos; se identifican tramos separados por más de diez minutos.

## 3 Calidad y particionamiento

| Resultado | Cantidad |
|---|---:|
| Entrada CSV | 15690 |
| Silver, lecturas conservadas | 12853 |
| Cuarentena | 2837 |
| Gold, minutos observados | 4225 |
| Tramos | 26 |

Los campos esenciales se validan por presencia, tipo y finitud. Se admiten humedad de 0 a 100 %, temperatura de −40 a 85 °C y presión de 300 a 1100 hPa como límites técnicos amplios, no de confort. Conforme al brief, gas no positivo se considera fallo y se excluye conservadoramente también de humedad. La regla puede rechazar lecturas útiles de otras variables y debe revisarse con el diagnóstico del hardware.

Se normalizan vacíos, se separan valores inválidos y se deduplica por dispositivo/fecha, conservando el menor ID numérico con desempate por contenido. Los datos reales no presentaron duplicados en las inspecciones iniciales; la prueba sintética verifica que la regla actúa cuando aparecen. Se muestran nulos antes/después y se prueban nueve casos de calidad más un caso de deduplicación.

Silver y Gold se particionan por fecha local de Lima. La lectura de vuelta reconcilia conteos y valores mediante `exceptAll` en ambos sentidos. Un filtro de fecha verifica `PartitionFilters` en el plan.

- [Manifiesto S3](../outputs/s03/20261120T030559821713Z/manifest.json).
- [Plan de particiones](../outputs/s03/20261120T030559821713Z/plan_partition_filters.txt).
- Parquet y cuarentena: `outputs/s03/20261120T030559821713Z`.

## 4 Componente ML distribuido

Objetivo: humedad promedio por minuto. Predictores: temperatura, presión, seno y coseno de la hora. El pipeline guarda las transformaciones, VectorAssembler y el estimador. No se incluyen la humedad objetivo ni índices derivados como predictores contemporáneos.

Separación cronológica: 2535 minutos de entrenamiento, 845 de validación y 845 de prueba. La configuración se elige por RMSE de validación; luego se ajusta con entrenamiento más validación y se evalúa una vez en prueba. Frente al ejemplo aleatorio de U1, esta decisión evita mezclar instantes cercanos entre bloques. La generalización sigue limitada a los periodos y al sensor disponibles.

### Comparación de validación

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| lineal | 5.6676 | 4.6483 | 0.3575 |
| ridge | 5.7108 | 4.6165 | 0.3477 |
| elastic_net | 5.7369 | 4.5862 | 0.3417 |
| random_forest | 4.3296 | 2.9952 | 0.6250 |

Configuración elegida: **random_forest**. Random Forest utiliza 50 árboles, profundidad máxima 6 y semilla 42. Lineal no penaliza; Ridge y Elastic Net usan regParam 0,1; Elastic Net usa elasticNetParam 0,5.

### Evaluación final independiente

| Método | RMSE | MAE | R² |
|---|---:|---:|---:|
| Modelo seleccionado | 4.0985 | 3.2175 | -2.6561 |
| Media de entrenamiento más validación | 3.2112 | 2.4930 | -1.2445 |

RMSE y MAE están en puntos porcentuales de humedad. El modelo pierde frente a la referencia en prueba. R² negativo significa que predice peor que la media del propio conjunto evaluado; no es un porcentaje de precisión. Los resultados sugieren limitada generalización temporal, pero no establecen su causa. Se preserva el resultado sin ajustar decisiones al test.

El modelo se guardó y recargó: diferencia máxima de predicciones **0.0**. La selección del ganador y su persistencia cumplen el experimento; no prueban utilidad operativa.

- [Manifiesto S4](../outputs/s04/20261120T030650065330Z/manifest.json).
- Modelo: `outputs/s04/20261120T030650065330Z/modelo_humedad_u1`.
- [Manifiesto integrado S5](../outputs/s04/20261120T030650065330Z/s05_manifest.json).

### Indicador descriptivo

El primer 60 % de minutos observados define p5 = **30.2831 %** y p95 = **42.9697 %**. Es una referencia estadística, no sanitaria. Su periodo está definido metodológicamente y pendiente de aprobación docente. No se han trasladado umbrales de IAQ a humedad.

Por cada día u hora se calcula `100 × lecturas válidas fuera del rango / lecturas válidas del grupo`. Se usa Silver para respetar el indicador por lectura; también se entrega la vista por minutos para evidenciar el efecto de la frecuencia. La media diaria por lecturas no equivale al promedio de tiempo. Los resultados descriptivos quedan en los tres directorios `descriptivo_*` de S4 y en las tablas del cuaderno.

## 5 Matriz de cumplimiento

| Criterio U1 | Evidencia | Estado |
|---|---|---|
| Arquitectura y dimensión propia | Sección 1 y cuaderno integrado | Documentado; defensa pendiente |
| PySpark, RDD y plan | Celdas S2 integradas | Ejecutado |
| Parquet particionado y lectura de vuelta | Manifiesto S3 y PartitionFilters | Verificado |
| Flujo batch reproducible | Cuaderno S5 desde CSV hasta modelo | Ejecutado de principio a fin |
| ML comparado con tres métricas y guardado | Tablas y prueba de recarga | Verificado; sin mejora en test |
| Evidencias y documentación | Notebooks, manifiestos y guion | Local; faltan capturas formales y publicación |
| Sustentación | Guion y preguntas | No evaluada |

No se asigna una nota: la defensa, la publicación, los topics y las evidencias visuales aún deben comprobarse. El paquete documenta el trabajo individual de humedad; no acredita las otras dimensiones del equipo.

## 6 Reproducción y pendientes

Seguir el [README](../README.md) y ejecutar `scripts/ejecutar_s05.ps1`. Cada corrida tiene su directorio; no reemplazar los manifiestos citados sin regenerar este informe. El script `scripts/documentar_s05.py` permite actualizarlo a partir de la última ejecución integrada completada.

Ensayar con el [guion de sustentación](guion-s05.md). Capturar pantallas reales sin recortar, con fecha/hora y perfil visibles. Integrar el trabajo revisado al repositorio del equipo, revisar topics y publicar documentación navegable. Ninguna captura ni firma de aprobación fue fabricada.

Fuentes académicas: [Producto U1](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/) y [Evaluación S5](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/).
