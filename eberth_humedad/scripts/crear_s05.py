"""Ensambla el cuaderno de U1 sin duplicar lógica de S2-S4."""
from pathlib import Path
import copy
import nbformat as n

root = Path(__file__).resolve().parents[1]
nb = n.v4.new_notebook()
nb.metadata.kernelspec = dict(display_name='Python 3 (ipykernel)', language='python', name='python3')
nb.cells = [n.v4.new_markdown_cell('''# AireIQ Unidad I integrada
**Dimensión:** humedad. **Responsable:** Eberth Emmanuel Rosales Castillo.

Ejecutar desde Kernel → Restart Kernel and Run All Cells. Este cuaderno une S2, S3 y S4 sobre el CSV original. Genera nuevas carpetas de resultados y conserva las anteriores. La fuente está montada en solo lectura.

## Arquitectura y alcance
Arquitectura Lambda: la ruta batch analiza el histórico y prepara modelos; la ruta streaming se implementará en U2. La fuente original es ESP32/BME680 → Supabase; esta evaluación usa su exportación CSV → PySpark → Parquet → MLlib. El laboratorio ejecuta Spark `local[2]`, no un clúster multinodo.

Pregunta descriptiva: ¿cómo varía la humedad por hora y día y qué porcentaje de lecturas válidas queda fuera de p5–p95?

Pregunta predictiva U1: ¿qué tan precisamente se estima la humedad con temperatura, presión y hora del mismo instante?

La automatización asiste la implementación; el estudiante debe revisar, comprender y defender las decisiones. El resultado del modelo se informará incluso si no supera una referencia sencilla.
''')]
for filename in ['01_entorno_humedad.ipynb', '03_calidad_humedad.ipynb', '04_modelos_humedad.ipynb']:
    part = n.read(root / 'notebooks' / filename, as_version=4)
    for original in part.cells:
        cell = copy.deepcopy(original)
        if cell.cell_type == 'code':
            cell.outputs = []
            cell.execution_count = None
            if filename.startswith('04_') and 'sources = sorted' in cell.source:
                cell.source = cell.source.replace('source = sources[-1]', 'source = s03_integrado\nassert source in sources, "La corrida S3 integrada no esta completa"')
        nb.cells.append(cell)
    if filename.startswith('03_'):
        nb.cells.append(n.v4.new_code_cell('''# Enlazar S4 con la salida de ESTA ejecución S3, aunque existan otras corridas.
s03_integrado = out
# Evidencia explícita de nulos y duplicados antes/después.
essential = ["id", "created_at", "dispositivo", "temperatura_c", "humedad_pct", "presion_hpa", "gas_ohms"]
print("Nulos o vacíos de entrada:")
raw.select(*[F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c) for c in essential]).show()
print("Nulos después:")
read_silver.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in essential]).show()
print("Duplicados antes:", raw.count() - raw.select("dispositivo", "created_at").distinct().count())
print("Duplicados después:", read_silver.count() - read_silver.select("dispositivo", "created_at").distinct().count())
# Prueba independiente de la prioridad de deduplicación con dos eventos repetidos.
first = raw.first().asDict()
test_dups = spark.createDataFrame([{**first, "id":"200"}, {**first, "id":"100"}], schema)
rank_test = validar(test_dups).filter(F.size("motivos") == 0).withColumn("orden", F.row_number().over(order))
assert rank_test.filter("orden = 1").select("id").first()[0] == "100"
assert rank_test.filter("orden > 1").count() == 1
print("Deduplicación determinista sintética: OK")
'''))
nb.cells.append(n.v4.new_code_cell('''# Manifiesto final de S5: solo se escribe después de superar todas las celdas.
import getpass
from IPython.display import display, Markdown
final_stage = final_model.stages[-1]
if hasattr(final_stage, "featureImportances"):
    importance = dict(zip(features, map(float, final_stage.featureImportances)))
    print("Importancias del modelo, no efectos causales:", importance)
else:
    importance = None
end_report = {"s03": str(s03_integrado), "s04": str(out), "completed_utc": datetime.now(timezone.utc).isoformat(),
              "executed_by_container_user": getpass.getuser(), "responsable_academico": "Eberth Emmanuel Rosales Castillo",
              "source_sha256": s03["source_sha256"], "modelo": winner, "test": test_metrics,
              "baseline_test": baseline_test, "importancias": importance, "duplicate_fixture_passed": True,
              "status": "ejecucion_tecnica_completa; sustentacion y capturas pendientes"}
(out / "s05_manifest.json").write_text(json.dumps(end_report, ensure_ascii=False, indent=2), encoding="utf-8")
display(Markdown("**Ejecución técnica terminada.** Revisar las métricas y sus limitaciones antes de sustentar."))
print(json.dumps(end_report, ensure_ascii=False, indent=2))
'''))
for cell in nb.cells: cell['id'] = n.v4.new_code_cell().id
n.validate(nb)
n.write(nb, root / 'notebooks/05_producto_u1_humedad.ipynb')
print('Cuaderno integrado S5 creado')
