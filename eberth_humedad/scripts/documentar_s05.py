"""Genera el producto U1 solo desde una ejecución integrada completada."""
from pathlib import Path
import json
from datetime import datetime, timedelta, timezone

root = Path(__file__).resolve().parents[1]
reports = sorted((root / 'outputs/s04').glob('*/s05_manifest.json'))
assert reports, 'No hay ejecucion S5 completa'
path = reports[-1]
end = json.loads(path.read_text(encoding='utf-8'))
s4dir = path.parent
s4 = json.loads((s4dir / 'manifest.json').read_text(encoding='utf-8'))
s3dir = root / 'outputs/s03' / s4['s03_run']
s3 = json.loads((s3dir / 'manifest.json').read_text(encoding='utf-8'))
def link(p): return '../' + p.relative_to(root).as_posix()
rows = '\n'.join(f"| {r['modelo']} | {r['rmse']:.4f} | {r['mae']:.4f} | {r['r2']:.4f} |" for r in s4['validacion'])
t, b = s4['test'], s4['baseline_test']
date = datetime.fromisoformat(end['completed_utc']).astimezone(timezone(timedelta(hours=-5))).strftime('%d/%m/%Y %H:%M:%S UTC-05')
text = f'''# Producto U1 de humedad en AireIQ

Responsable académico: Eberth Emmanuel Rosales Castillo, equipo Novoa. Ejecución integrada verificada el {date}. La estimación de humedad funciona técnicamente, pero el modelo seleccionado no superó la referencia de media en prueba. No se recomienda como mejora predictiva en producción con esta evidencia.

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
| Entrada CSV | {s3['entrada']} |
| Silver, lecturas conservadas | {s3['silver']} |
| Cuarentena | {s3['cuarentena']} |
| Gold, minutos observados | {s3['gold_minutos']} |
| Tramos | {s3['tramos']} |

Los campos esenciales se validan por presencia, tipo y finitud. Se admiten humedad de 0 a 100 %, temperatura de −40 a 85 °C y presión de 300 a 1100 hPa como límites técnicos amplios, no de confort. Conforme al brief, gas no positivo se considera fallo y se excluye conservadoramente también de humedad. La regla puede rechazar lecturas útiles de otras variables y debe revisarse con el diagnóstico del hardware.

Se normalizan vacíos, se separan valores inválidos y se deduplica por dispositivo/fecha, conservando el menor ID numérico con desempate por contenido. Los datos reales no presentaron duplicados en las inspecciones iniciales; la prueba sintética verifica que la regla actúa cuando aparecen. Se muestran nulos antes/después y se prueban nueve casos de calidad más un caso de deduplicación.

Silver y Gold se particionan por fecha local de Lima. La lectura de vuelta reconcilia conteos y valores mediante `exceptAll` en ambos sentidos. Un filtro de fecha verifica `PartitionFilters` en el plan.

- [Manifiesto S3]({link(s3dir / 'manifest.json')}).
- [Plan de particiones]({link(s3dir / 'plan_partition_filters.txt')}).
- Parquet y cuarentena: `{s3dir.relative_to(root).as_posix()}`.

## 4 Componente ML distribuido

Objetivo: humedad promedio por minuto. Predictores: temperatura, presión, seno y coseno de la hora. El pipeline guarda las transformaciones, VectorAssembler y el estimador. No se incluyen la humedad objetivo ni índices derivados como predictores contemporáneos.

Separación cronológica: {s4['conteos']['train']} minutos de entrenamiento, {s4['conteos']['validation']} de validación y {s4['conteos']['test']} de prueba. La configuración se elige por RMSE de validación; luego se ajusta con entrenamiento más validación y se evalúa una vez en prueba. Frente al ejemplo aleatorio de U1, esta decisión evita mezclar instantes cercanos entre bloques. La generalización sigue limitada a los periodos y al sensor disponibles.

### Comparación de validación

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
{rows}

Configuración elegida: **{s4['modelo_elegido']}**. Random Forest utiliza 50 árboles, profundidad máxima 6 y semilla 42. Lineal no penaliza; Ridge y Elastic Net usan regParam 0,1; Elastic Net usa elasticNetParam 0,5.

### Evaluación final independiente

| Método | RMSE | MAE | R² |
|---|---:|---:|---:|
| Modelo seleccionado | {t['rmse']:.4f} | {t['mae']:.4f} | {t['r2']:.4f} |
| Media de entrenamiento más validación | {b['rmse']:.4f} | {b['mae']:.4f} | {b['r2']:.4f} |

RMSE y MAE están en puntos porcentuales de humedad. El modelo pierde frente a la referencia en prueba. R² negativo significa que predice peor que la media del propio conjunto evaluado; no es un porcentaje de precisión. Los resultados sugieren limitada generalización temporal, pero no establecen su causa. Se preserva el resultado sin ajustar decisiones al test.

El modelo se guardó y recargó: diferencia máxima de predicciones **{s4['diferencia_recarga']}**. La selección del ganador y su persistencia cumplen el experimento; no prueban utilidad operativa.

- [Manifiesto S4]({link(s4dir / 'manifest.json')}).
- Modelo: `{s4dir.relative_to(root).as_posix()}/modelo_humedad_u1`.
- [Manifiesto integrado S5]({link(path)}).

### Indicador descriptivo

El primer 60 % de minutos observados define p5 = **{s4['p5']:.4f} %** y p95 = **{s4['p95']:.4f} %**. Es una referencia estadística, no sanitaria. Su periodo está definido metodológicamente y pendiente de aprobación docente. No se han trasladado umbrales de IAQ a humedad.

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
'''
(root / 'docs/u1-producto.md').write_text(text, encoding='utf-8')
print('Producto U1 generado desde', path.name, s4dir.name)
