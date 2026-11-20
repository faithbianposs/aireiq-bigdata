"""Derive a separate, reviewable 120-second experiment from the verified S10 notebook."""
from pathlib import Path
import nbformat as n

root=Path(__file__).resolve().parents[1]
nb=n.read(root/'notebooks/10_pronostico_humedad.ipynb',as_version=4)
for cell in nb.cells:
    if cell.cell_type=='code':
        cell.outputs=[];cell.execution_count=None
        cell.source=cell.source.replace("root/'s10'/", "root/'s10_h120'/")
        cell.source=cell.source.replace("F.lead('minuto').over(w)", "F.lead('minuto',2).over(w)")
        cell.source=cell.source.replace("F.lead('humedad_pct').over(w)", "F.lead('humedad_pct',2).over(w)")
        cell.source=cell.source.replace("cast('long')==60", "cast('long')==120")
        cell.source=cell.source.replace("horizonte_segundos=60", "horizonte_segundos=120")
        cell.source=cell.source.replace("objetivo='humedad media del siguiente minuto'", "objetivo='humedad media del minuto que inicia 120 segundos despues del minuto de entrada'")
    else:
        cell.source=''
nb.cells=[c for c in nb.cells if c.source.strip()]
nb.cells.insert(0,n.v4.new_markdown_cell('''# Humedad: horizonte de 120 segundos

Experimento separado de S10. Se predice la media del minuto t+2 usando las medias del minuto t. Los pares requieren que la segunda fila posterior del mismo dispositivo y tramo tenga una separación exacta de 120 s. No se rellenan huecos. Cortes cronológicos 60/20/20, persistencia, regresión lineal y Random Forest; selección en validación, test independiente y recarga del modelo.

El horizonte se mide entre los inicios de los minutos, no desde la publicación. Si t queda disponible en t+70 s, quedan aproximadamente 50 s hasta que empiece el minuto objetivo; se medirá el margen real. Una predicción emitida después de ese inicio se marca tardía y no cuenta como anticipación.

La evaluación usa Gold histórico completo. Los minutos vivos pueden estar incompletos por el plazo de recepción: estas métricas no acreditan todavía rendimiento con ese cambio de distribución.
'''))
n.validate(nb)
target=root/'notebooks/10_pronostico_humedad_120s.ipynb'
n.write(nb,target)
print(target)
