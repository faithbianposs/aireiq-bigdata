# Pronóstico de humedad con horizonte de 120 segundos

Se mantiene S10 de un minuto como experimento previo. Este nuevo experimento estima la media del minuto que comienza 120 segundos después del inicio del minuto de entrada. No significa dos minutos de anticipación desde la emisión: si la media de entrada está disponible en t+70 s, quedan aproximadamente 50 s antes de que comience el minuto objetivo, menos el tiempo de inferencia.

## Evidencia de disponibilidad

El adaptador publicó dos minutos reales del dispositivo ESP32_AireIQ: 2026-11-20 13:53 UTC y 14:03 UTC. Sus demoras de cierre fueron 10,190 y 10,300 segundos, respectivamente. Cada uno contiene una lectura. Los diez minutos de separación no se rellenan; con esta muestra no se valida todavía continuidad, completitud ni precisión predictiva en operación.

## Evaluación independiente

El cuaderno `notebooks/10_pronostico_humedad_120s.ipynb` genera pares exactos a 120 segundos dentro del mismo dispositivo y tramo. Se utiliza la segunda fila posterior y se exige la separación exacta; no se confunde siguiente registro con tiempo transcurrido. De 4 225 minutos se obtuvieron 4 018 pares; cuatro se excluyeron por cruzar cortes. División cronológica: 2 408 entrenamiento, 802 validación y 804 prueba.

| Método | RMSE validación | MAE validación |
|---|---:|---:|
| Persistencia | 0,563879 | 0,257434 |
| Regresión lineal regularizada | 0,624484 | 0,363863 |
| Random Forest | 3,260369 | 2,134600 |

Persistencia ganó en validación. En prueba: RMSE 0,331372, MAE 0,120143 puntos porcentuales, R² 0,968241. Mejora frente a persistencia: cero, porque la referencia fue la seleccionada. La recarga del PipelineModel produjo diferencia máxima cero. No se cambió la selección después de observar el test.

Manifiesto: `outputs/s10_h120/20261120T140558756871Z/manifest.json`. Cuaderno ejecutado: `outputs/10_pronostico_humedad_120s_ejecutado.ipynb`. El generador `scripts/crear_s10_h120.py` parte del cuaderno S10 y guarda un archivo separado; el entrenamiento se ejecuta con `scripts/entrenar_h120_visible.ps1`.

## Condiciones antes de la integración continua

- Registrar `issued_at` después de completar la inferencia y comparar con el inicio del minuto objetivo. Si ya comenzó, clasificar el resultado como tardío; excluirlo de la evaluación de anticipación.
- Evaluar solo cuando exista el minuto objetivo exacto del mismo dispositivo; nunca usar la siguiente observación si hay huecos.
- Conservar por separado el error sobre minutos vivos posiblemente incompletos y el test de Gold histórico completo. No atribuir al flujo vivo las métricas de esta tabla.
- Medir más muestras de retraso antes de ajustar el plazo del adaptador. El cierre de diez segundos sigue siendo provisional.

La prueba `scripts/verificar_h120_visible.ps1` reproduce 100 entradas históricas por Kafka, sin etiquetas futuras, y contrasta la salida Spark Streaming con la inferencia batch. Solo un manifiesto `status=passed` en `outputs/s10_h120_streaming/` confirma esa prueba; sigue siendo reproducción histórica, no inferencia sobre el sensor actual.

La corrida `b90b5f83ef8d4f7b8ac2873ba1c33261` pasó: 100 predicciones, objetivos separados exactamente 120 segundos y diferencia máxima batch/streaming cero. RMSE retrospectivo del subconjunto: 0,289628; MAE: 0,142016 puntos porcentuales. Estos valores no sustituyen el test completo de 804 pares ni acreditan error medido en vivo.
