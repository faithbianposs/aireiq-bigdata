# Sustentación de humedad en AireIQ

Responsable: Eberth Emmanuel Rosales Castillo. Este guion prepara la defensa individual de U1. La implementación fue asistida por Codex: revisar el código, ensayar y explicar las decisiones con palabras propias. No atribuirse pruebas, capturas o commits que no se hayan realizado.

## Presentación técnica de ocho minutos

| Tiempo | Explicación y evidencia |
|---|---|
| 0:00–1:00 | Problema: disponer de lecturas no basta para describir la humedad ni evaluar su estimación. Presentar las preguntas descriptiva y predictiva U1. |
| 1:00–2:00 | Arquitectura Lambda. Dibujar ESP32 → Supabase → CSV → Spark → Parquet → MLlib. La rama Kafka/streaming pertenece a U2 y está pendiente. |
| 2:00–3:30 | Mostrar esquema, nulos, deduplicación y cuarentena. El gas igual a cero se usa como señal de fallo conforme al brief; no se elimina automáticamente cualquier humedad del 100 %. |
| 3:30–4:30 | Explicar Silver por lectura y Gold por minuto. Partición por fecha de Lima; no por ID. Mostrar reconciliación y PartitionFilters. |
| 4:30–6:00 | Explicar objetivo contemporáneo y predictores. Hora cíclica, VectorAssembler, cuatro modelos y separación cronológica 60/20/20. |
| 6:00–7:00 | Mostrar comparación de validación y prueba final. Random Forest ganó en validación, pero no superó la media de entrenamiento en prueba. No cambiar el ganador después de mirar el test. |
| 7:00–8:00 | Mostrar modelo guardado/recargado y límites: un sensor, huecos y fallo predictivo fuera del entrenamiento. El p5–p95 es estadístico; falta aprobación del periodo de referencia. |

## Demo técnica de cinco minutos

Abrir `05_producto_u1_humedad.ipynb` en JupyterLab. Conservar también la copia ejecutada. Ensayar la duración en esta computadora: no garantizar que todo el entrenamiento termine dentro de cinco minutos.

1. Mostrar una transformación y `explain(True)`, distinguiendo transformación de acción.
2. Ejecutar los conteos de calidad y mostrar una fila en cuarentena. Mostrar la prueba sintética de duplicados, diferenciándola de la fuente real.
3. Mostrar escritura/lectura Parquet y el plan con `PartitionFilters`.
4. Mostrar RMSE, MAE y R² de validación y del test. La referencia de media se calcula sin usar los valores del test.
5. Mostrar la selección, guardado y recarga del modelo. La diferencia de predicciones debe ser cero o quedar dentro de la tolerancia declarada.

Si se ejecutan celdas clave, primero correr el cuaderno completo en orden para disponer de variables. No ejecutar celdas fuera de orden sobre estados desconocidos. Para una reproducción limpia, reiniciar el kernel y ejecutar todas.

## Preguntas para practicar durante cinco minutos

**¿Por qué Lambda?** El caso necesita análisis histórico y una ruta futura de eventos. Kappa podría reprocesar un registro de eventos, pero aquí ya existe un histórico exportado de Supabase y se construyen ambas capacidades por etapas.

**¿Cuándo calcula Spark?** `filter` y `withColumn` describen transformaciones. `count`, `show` y la escritura solicitan resultados. `explain` muestra el plan; las acciones anteriores pueden haber materializado datos en caché.

**¿Qué provocan los duplicados?** Sobrerrepresentan lecturas y pueden multiplicar filas al unir datasets. La clave dispositivo/fecha y un desempate determinista permiten un resultado repetible.

**¿Por qué fecha como partición?** Permite limitar archivos al consultar periodos y tiene cardinalidad razonable. Un ID único produciría muchas particiones pequeñas.

**¿Para qué VectorAssembler?** Reúne los predictores en la columna vectorial que esperan estos estimadores de Spark ML. También puede construirse ese vector por otros medios; lo obligatorio para estos modelos es el formato de entrada.

**¿Qué diferencia hay entre fit y transform?** `fit` aprende parámetros con entrenamiento; `transform` aplica lo aprendido a nuevas filas sin reentrenar.

**¿Qué hace la regularización?** Ridge penaliza coeficientes grandes mediante L2; Elastic Net combina L1 y L2. Sus parámetros se comparan en validación.

**¿Por qué R² negativo?** La suma de errores cuadrados supera la variabilidad respecto a la media del propio periodo evaluado. No significa un porcentaje negativo de precisión.

**¿Por qué guardar Random Forest si pierde frente a la referencia?** Se conserva el mejor modelo ML según la regla de selección de validación para reproducir el experimento. No se recomienda como mejora predictiva en producción. La comparación final debe reportarse completa.

**¿La importancia de una variable indica causalidad?** No. Es una medida del uso de la variable en ese modelo y dataset; no demuestra que cambiarla cause un cambio en humedad.

**¿Es ya un pronóstico de un minuto?** No. U1 estima humedad del mismo instante. U2 requerirá etiquetas futuras, pares separados exactamente un minuto y referencia de persistencia.

## Capturas y autoría pendientes

La guía S5 exige capturas sin recortar, con reloj del sistema y usuario o perfil visibles. Preparar capturas reales de arquitectura, esquema/calidad, Parquet/PartitionFilters, comparación de modelos y recarga. No añadir un reloj o nombre artificialmente. El usuario del contenedor no sustituye la identidad visible solicitada por el docente.

También faltan integrar el trabajo revisado al repositorio del equipo, registrar commits propios, comprobar topics académicos y publicar documentación navegable. No se ha calificado la sustentación ni certificado aprobación docente.

Fuente: [Evaluación oficial S5](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/).
