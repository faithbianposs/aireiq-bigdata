# S10 Pronóstico de humedad a un minuto

Se entrenó un pipeline distinto del modelo contemporáneo U1. Su objetivo es la humedad media de t+1 minuto, usando humedad, temperatura, presión y hora del minuto t. Una entrada agregada basta para aplicar el modelo sin guardar historial de características en memoria.

## Preparación y evaluación

De 4 225 minutos Gold se obtuvieron 4 113 pares exactamente consecutivos del mismo dispositivo y tramo. Se excluyeron dos pares que cruzaban los cortes cronológicos. Quedaron 2 466 pares de entrenamiento, 822 de validación y 823 de prueba. No se rellenaron huecos ni se confundió «siguiente fila» con «siguiente minuto».

| Método | RMSE validación | MAE validación | R² validación |
|---|---:|---:|---:|
| Persistencia | 0,36749 | 0,16115 | 0,99724 |
| Regresión lineal regularizada | 0,46021 | 0,28966 | 0,99567 |
| Random Forest | 3,29248 | 2,09753 | 0,77836 |

Ganó **persistencia**: pronostica que la humedad del próximo minuto será igual a la actual. En prueba obtuvo RMSE **0,21926**, MAE **0,08731** y R² **0,98722**. No hay mejora sobre persistencia porque ella misma fue elegida. RMSE/MAE se expresan en puntos porcentuales, no en porcentaje de precisión. Un R² alto en este horizonte no acredita generalización a otros sensores, ambientes o periodos.

La referencia también se conserva como PipelineModel de Spark, mediante una transformación SQL. Guardar ese pipeline permite aplicar el mismo contrato en batch y streaming, pero no significa que persistencia haya aprendido una relación predictiva compleja.

## Prueba con Kafka

Se enviaron 100 observaciones del periodo de prueba a un topic aislado `aireiq-s10-minutos-<corrida>`, sin publicar la etiqueta futura. Spark Structured Streaming cargó el pipeline guardado, aplicó `transform`, añadió el instante objetivo a 60 segundos y escribió Parquet con checkpoint.

Las 100 predicciones coincidieron con el cálculo batch: diferencia máxima cero. El RMSE retrospectivo de ese subconjunto fue 0,06715 y el MAE 0,04632; no sustituyen las métricas de las 823 observaciones del test completo.

Evidencias: `outputs/s10/20261120T040857589605Z/manifest.json` y `outputs/s10_streaming/8494001f31cc42bfadc26c1b869502d0/manifest.json`. El cuaderno ejecutado está en `outputs/10_pronostico_humedad_ejecutado.ipynb`.

## Granularidad y latencia

Se entrenó con promedios por minuto y la prueba mantuvo esa granularidad. No se debe conectar directamente una lectura instantánea de 16 segundos sin validar ese cambio de distribución. Tampoco se debe alimentar un pronóstico a un minuto desde la salida S8 que espera dos minutos de watermark: llegaría tarde.

La ruta actual usa minutos históricos de Gold. Para datos nuevos se necesita un adaptador que emita cada minuto cerrado con su tiempo de disponibilidad explícito, y revisar cómo tratar tardíos sin introducir conocimiento futuro. Esa conexión continua no se ha probado.

## Ejecutar

```powershell
powershell -NoProfile -NoExit -ExecutionPolicy Bypass -File scripts/demo_s10_visible.ps1
```

El script vuelve a entrenar y crea nuevas carpetas. Para estudiar el procedimiento sin repetir la ejecución, abrir `10_pronostico_humedad.ipynb` en JupyterLab o su copia ejecutada. La reproducción Kafka S10 termina tras procesar lo disponible; no es un servicio de pronóstico conectado al ESP32.
