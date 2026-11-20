# S11 BI y error de pronóstico de humedad

El tablero [AireIQ Humedad batch y pronóstico](http://localhost:43000/d/aireiq-bi) reúne métricas U1, resultados S10, humedad observada, pronóstico y error calculado durante una reproducción de eventos históricos. No representa un sensor conectado en vivo.

## Flujo

```text
Minutos Gold del periodo de prueba → Kafka
→ Spark Structured Streaming → pipeline S10 guardado
→ registro de predicciones y observaciones en SQLite
→ Pushgateway → Prometheus → Grafana
```

La demostración publica 60 minutos, uno cada medio segundo. No envía etiquetas futuras. Cada predicción se guarda con `target_time = minuto + 60 s`. Cuando llega una observación que coincide con ese objetivo y dispositivo, se calcula su error. No se compara una predicción futura contra la humedad del instante equivocado.

Se usa SQLite por corrida como registro persistente de demostración. IDs únicos y transacciones impiden duplicar las observaciones al repetir un microbatch; el score es único por dispositivo y tiempo objetivo. El consumo se ordena por offset de una partición. El modelo aplica inferencia sin historial; la evaluación sí conserva las predicciones pendientes para emparejarlas con observaciones posteriores.

El sink recorre filas en el driver y es adecuado para esta demostración pequeña. No acredita escalado a grandes volúmenes ni manejo completo de desorden: un valor real que llegue antes que su predicción requeriría reconciliación adicional. Kafka no tiene transacción atómica con SQLite y Pushgateway; las garantías de publicación deben revisarse antes de producción.

## Tablero e interpretación

Los paneles batch muestran el RMSE contemporáneo U1 y el RMSE del test de pronóstico S10. Tienen objetivos y poblaciones distintas, por lo que no se interpreta su diferencia como mejora de un modelo sobre otro.

El gráfico de humedad observada se compara con la predicción anterior destinada a ese mismo minuto. Un panel separado muestra el pronóstico del minuto siguiente. El eje temporal de Grafana es el reloj de reproducción, no el histórico original.

RMSE y MAE acumulados se calculan únicamente con predicciones cuyo valor real ya llegó. La última predicción puede quedar pendiente si se termina la demostración. Los errores de este subconjunto no reemplazan las métricas del test completo S10.

La alerta estadística compara el pronóstico con p5/p95 definidos en U1; no es un umbral sanitario ni demuestra que sea necesario ventilar. El periodo de referencia sigue pendiente de aprobación académica.

## Pushgateway

Se usa Pushgateway 1.11.1, versión del entorno del docente, como mecanismo pragmático de publicación. No es el patrón recomendado para monitorizar un servicio persistente; S9 conserva scraping directo para salud operativa.

Pushgateway mantiene las métricas después de terminar el productor. Por eso el tablero expone `aireiq_bi_active` y la antigüedad de actualización. `active=0` significa que la demo terminó, aunque sus valores sigan visibles. Una nueva demo reemplaza el grupo de métricas `job=aireiq_bi`; los registros y manifiestos de cada corrida se conservan por separado.

## Ejecución y evidencia

```powershell
powershell -NoProfile -NoExit -ExecutionPolicy Bypass -File scripts/demo_s11_visible.ps1
```

La ventana levanta Pushgateway, carga el nuevo datasource de scraping y el tablero, ejecuta la reproducción y verifica las consultas a través de Grafana. Las salidas están en `outputs/s11/`: logs visibles, manifiestos por corrida y la base `evaluacion.sqlite`. Solo un manifiesto `status=passed` confirma sus verificaciones; la revisión visual se distingue de la validación funcional por API.

### Resultado verificado

La corrida `717f6a0e1bca4f1a894c904f70594926` terminó con `status=passed`: procesó 60 observaciones y evaluó 59 predicciones al llegar su valor real. La última predicción quedó pendiente. El RMSE fue 0,053823 puntos porcentuales, igual al de persistencia, que es el modelo seleccionado. Este resultado corresponde únicamente al subconjunto reproducido.

El manifiesto `outputs/s11/717f6a0e1bca4f1a894c904f70594926/manifest.json` registra la prueba. La verificación `outputs/s11/dashboard-86f0c54d681540549a42357a5e8e4aea/manifest.json` confirmó las consultas mediante el datasource de Grafana: 60 observaciones, 59 evaluaciones y el mismo RMSE. Fue una verificación funcional por API; no se realizó revisión visual del tablero.

La integración continua con Supabase, la generación de minutos nuevos y la validación del tiempo real de disponibilidad siguen pendientes. No se ha subido código a GitHub ni se requieren capturas de tarea en este alcance.
