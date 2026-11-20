"""Genera el tablero Grafana 'AireIQ · Humedad y predicción real' (uid aireiq-vivo).

Escribe infra/grafana/dashboards/live.json y, si existe el repo del equipo, también
../grafana/dashboards/aireiq_humedad.json para que ambos queden idénticos.
"""
import json
from pathlib import Path

DS = {'type': 'prometheus', 'uid': '${ds}'}
GREEN = {'mode': 'absolute', 'steps': [{'color': 'green', 'value': None}]}
panels = []
_next_id = [1]


def pid():
    _next_id[0] += 1
    return _next_id[0]


def thresholds(*steps):
    """steps: (valor, color) desde el segundo escalón; el primero siempre es verde."""
    return {'mode': 'absolute', 'steps': [{'color': 'green', 'value': None}] +
            [{'color': c, 'value': v} for v, c in steps]}


def stat(title, expr, x, y, w=6, h=4, unit='short', decimals=None, th=GREEN, desc=None, legend=None):
    defaults = {'unit': unit, 'color': {'mode': 'thresholds'}, 'thresholds': th}
    if decimals is not None:
        defaults['decimals'] = decimals
    p = {'id': pid(), 'type': 'stat', 'title': title, 'datasource': DS,
         'gridPos': {'x': x, 'y': y, 'w': w, 'h': h},
         'targets': [{'refId': 'A', 'datasource': DS, 'expr': expr, 'range': True,
                      'legendFormat': legend or title}],
         'fieldConfig': {'defaults': defaults, 'overrides': []},
         'options': {'graphMode': 'area', 'colorMode': 'value', 'justifyMode': 'center',
                     'textMode': 'value', 'orientation': 'auto', 'wideLayout': True,
                     'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}}
    if desc:
        p['description'] = desc
    panels.append(p)


def series(title, targets, x, y, w=12, h=8, unit='short', desc=None, th=None, extra=None):
    defaults = {'unit': unit, 'color': {'mode': 'palette-classic'},
                'custom': {'lineWidth': 2, 'fillOpacity': 10, 'showPoints': 'never',
                           'spanNulls': True, 'drawStyle': 'line'}}
    if th:
        defaults['thresholds'] = th
        defaults['custom']['thresholdsStyle'] = {'mode': 'dashed'}
    p = {'id': pid(), 'type': 'timeseries', 'title': title, 'datasource': DS,
         'gridPos': {'x': x, 'y': y, 'w': w, 'h': h},
         'targets': [{'refId': chr(65 + i), 'datasource': DS, 'expr': e, 'range': True, 'legendFormat': l}
                     for i, (e, l) in enumerate(targets)],
         'fieldConfig': {'defaults': defaults, 'overrides': extra or []},
         'options': {'legend': {'displayMode': 'list', 'placement': 'bottom', 'showLegend': True},
                     'tooltip': {'mode': 'multi', 'sort': 'none'}}}
    if desc:
        p['description'] = desc
    panels.append(p)


def row(title, y):
    panels.append({'id': pid(), 'type': 'row', 'title': title, 'collapsed': False,
                   'gridPos': {'x': 0, 'y': y, 'w': 24, 'h': 1}, 'panels': []})


def color(name, c):
    return {'matcher': {'id': 'byName', 'options': name},
            'properties': [{'id': 'color', 'value': {'mode': 'fixed', 'fixedColor': c}}]}


# ── Fila superior: salud del flujo ─────────────────────────────────────────────
stat('Segundos desde el último push (inferencia)', 'time() - aireiq_live_updated_timestamp', 0, 0, w=5, h=6,
     unit='s', decimals=0, th=thresholds((30, 'orange'), (60, 'red')),
     desc='¿Está vivo el servicio? Tiempo desde que live_forecast.py publicó métricas en Pushgateway. Verde < 30 s.')
stat('Antigüedad del último dato del sensor', 'time() - aireiq_live_event_timestamp', 5, 0, w=5, h=6,
     unit='s', decimals=0, th=thresholds((180, 'orange'), (300, 'red')),
     desc='¿Llegan datos del ESP32? Tiempo desde el inicio del último minuto observado. Lo normal es 70–135 s '
          '(el minuto se cierra a los 60 s + 10 s de tolerancia + sondeo de 15 s). Naranja > 3 min, rojo > 5 min: '
          'el servicio sigue vivo pero el sensor dejó de enviar.')
stat('Modelo en producción (horizonte 120 s)', 'aireiq_live_model_info', 10, 0, w=6, h=6,
     legend='{{modelo}}',
     desc='PipelineModel MLlib cargado por live_forecast.py (outputs/s10_h120/<corrida>/modelo). '
          'Persistencia ganó la validación cronológica a 120 s frente a regresión lineal y Random Forest. '
          'La corrida exacta está en la etiqueta "corrida" de la métrica aireiq_live_model_info.')
panels[-1]['options'].update(textMode='name', graphMode='none', colorMode='value',
                             text={'valueSize': 40})
series('Minutos procesados (eventos de aireiq-minutos-vivos-v1)',
       [('aireiq_live_observations', 'minutos'), ('aireiq_live_timely_predictions', 'predicciones oportunas')],
       16, 0, w=8, h=6, desc='Acumulado de minutos consumidos de Kafka por Spark y de predicciones emitidas a tiempo.')

# ── Dimensión humedad ──────────────────────────────────────────────────────────
row('Dimensión Humedad: humedad media a +120 s', 6)
stat('Humedad último minuto (%)', 'aireiq_live_humidity_actual', 0, 7, unit='none', decimals=1)
stat('Predicción humedad a +120 s (%)', 'aireiq_live_humidity_prediction', 6, 7, unit='none', decimals=1,
     th=thresholds((42, 'orange')), desc='Naranja si supera p95 = 42 % (umbral de intervención).')
stat('MAE modelo: persistencia (pp)', 'aireiq_live_mae', 12, 7, unit='none', decimals=3,
     desc='Error absoluto medio vivo; solo pares con predicción emitida antes del minuto objetivo.')
stat('MAE persistencia (baseline, pp)', 'aireiq_live_baseline_mae', 18, 7, unit='none', decimals=3,
     desc='Mismo cálculo usando como predicción la humedad del minuto de entrada.')
stat('Mejora vs persistencia', 'aireiq_live_improvement_pct', 0, 11, unit='percent', decimals=1,
     th=thresholds((-1e9, 'red'), (0, 'green')),
     desc='(1 − MAE modelo / MAE persistencia) × 100. Persistencia fue el modelo seleccionado, por eso ≈ 0 %.')
panels[-1]['fieldConfig']['defaults']['thresholds'] = {'mode': 'absolute', 'steps': [
    {'color': 'red', 'value': None}, {'color': 'green', 'value': 0}]}
stat('Pares evaluados', 'aireiq_live_scored', 6, 11, decimals=0)
stat('Intervenciones (> umbral p95)', 'aireiq_live_interventions_emitted', 12, 11, decimals=0,
     desc='Eventos ventilacion_preventiva_sugerida publicados en aireiq-intervenciones-v1.')
stat('Minutos descartados (tiempo inválido)', 'aireiq_live_discarded_timing', 18, 11, decimals=0,
     desc='Minutos rechazados por llegar tarde o con marca de tiempo inválida, desde el último arranque del servicio.')
series('Humedad: real vs predicción a +120 s',
       [('aireiq_live_humidity_actual', 'real'),
        ('aireiq_live_humidity_prediction offset 2m', 'predicción (alineada al minuto objetivo)'),
        ('aireiq_live_humidity_prediction', 'predicción emitida')],
       0, 15, w=12, h=9, unit='none', th=thresholds((42, 'orange')),
       extra=[color('real', 'yellow'), color('predicción (alineada al minuto objetivo)', 'green'),
              {'matcher': {'id': 'byName', 'options': 'predicción emitida'},
               'properties': [{'id': 'custom.hideFrom', 'value': {'legend': False, 'tooltip': False, 'viz': True}}]}],
       desc='La predicción se desplaza 120 s para compararla con la humedad real del minuto que pronosticó. Línea discontinua: umbral 42 %.')
series('Error: modelo vs persistencia (acumulado)',
       [('aireiq_live_mae', 'MAE modelo'), ('aireiq_live_baseline_mae', 'MAE persistencia'),
        ('aireiq_live_rmse', 'RMSE vivo'), ('aireiq_live_batch_test_rmse', 'RMSE test histórico')],
       12, 15, w=12, h=9, unit='none',
       extra=[color('MAE modelo', 'green'), color('MAE persistencia', 'yellow'),
              color('RMSE vivo', 'blue'), color('RMSE test histórico', 'purple')])

# ── Anticipación y eventos de negocio ──────────────────────────────────────────
row('Anticipación y eventos de negocio', 24)
stat('Margen antes del minuto objetivo (s)', 'aireiq_live_lead_seconds', 0, 25, unit='s', decimals=1,
     th=thresholds((-1e9, 'red'), (0, 'green')),
     desc='Segundos entre la emisión de la predicción y el inicio del minuto objetivo. Debe ser > 0.')
panels[-1]['fieldConfig']['defaults']['thresholds'] = {'mode': 'absolute', 'steps': [
    {'color': 'red', 'value': None}, {'color': 'green', 'value': 0}]}
stat('Alertas suprimidas por cooldown (300 s)', 'aireiq_live_interventions_suppressed', 6, 25, decimals=0)
stat('Predicciones tardías excluidas', 'aireiq_live_late_predictions', 12, 25, decimals=0,
     th=thresholds((1, 'orange')))
stat('Lecturas del último minuto', 'aireiq_live_readings_in_minute', 18, 25, decimals=0)

# ── Kafka ──────────────────────────────────────────────────────────────────────
row('Kafka (kafka-exporter)', 29)
stat('Brokers Kafka activos', 'kafka_brokers', 0, 30, w=6, h=7, decimals=0,
     th={'mode': 'absolute', 'steps': [{'color': 'red', 'value': None}, {'color': 'green', 'value': 1}]})
series('Mensajes por segundo por topic AireIQ',
       [('sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"aireiq-.*"}[1m]))', '{{topic}}')],
       6, 30, w=9, h=7)
series('Lag de grupos de consumo AireIQ',
       [('sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~"aireiq-.*"})', '{{consumergroup}} · {{topic}}')],
       15, 30, w=9, h=7)
panels.append({'id': pid(), 'type': 'bargauge', 'title': 'Mensajes acumulados por topic y partición', 'datasource': DS,
               'gridPos': {'x': 0, 'y': 37, 'w': 24, 'h': 6},
               'targets': [{'refId': 'A', 'datasource': DS, 'instant': True,
                            'expr': 'kafka_topic_partition_current_offset{topic=~"aireiq-.*"}',
                            'legendFormat': '{{topic}} p{{partition}}'}],
               'fieldConfig': {'defaults': {'unit': 'short', 'color': {'mode': 'thresholds'}, 'thresholds': GREEN},
                               'overrides': []},
               'options': {'orientation': 'horizontal', 'displayMode': 'gradient', 'namePlacement': 'left',
                           'valueMode': 'color', 'text': {'titleSize': 14, 'valueSize': 16}, 'minVizHeight': 16,
                           'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}})

# ── Nota metodológica ──────────────────────────────────────────────────────────
panels.append({'id': pid(), 'type': 'text', 'title': 'Cómo leer este tablero', 'gridPos': {'x': 0, 'y': 43, 'w': 24, 'h': 4},
               'options': {'mode': 'markdown', 'content':
                   '**ESP32 → Supabase → Kafka (`aireiq-minutos-vivos-v1`) → Spark Structured Streaming + MLlib → '
                   'Pushgateway → Prometheus → Grafana**, y eventos de ventilación preventiva con cooldown 300 s a '
                   '`aireiq-intervenciones-v1`. Horizonte: inicio del minuto de entrada + 120 s. Solo cuentan las '
                   'predicciones emitidas antes del minuto objetivo. Sin pares evaluados los paneles de error quedan '
                   'vacíos (no se muestran ceros inventados). Persistencia fue el modelo seleccionado en validación.'}})

# ── BI histórico: consultas SQL directas a Supabase (rol de solo lectura) ─────
PG = {'type': 'grafana-postgresql-datasource', 'uid': 'aireiq-supabase'}
TABLE = 'public.mediciones_aire'
VALID = ('humedad_pct between 0 and 100 and gas_ohms > 0 and temperatura_c between -40 and 85 '
         'and presion_hpa between 300 and 1100')
LIMA = "created_at at time zone 'America/Lima'"
P5, P95 = 30.2831, 42.9697


def sql_panel(kind, title, sql, x, y, w, h, unit='short', decimals=None, desc=None, defaults=None, options=None,
              overrides=None):
    d = {'unit': unit, 'color': {'mode': 'thresholds'}, 'thresholds': GREEN}
    if decimals is not None:
        d['decimals'] = decimals
    d.update(defaults or {})
    p = {'id': pid(), 'type': kind, 'title': title, 'datasource': PG,
         'gridPos': {'x': x, 'y': y, 'w': w, 'h': h},
         'targets': [{'refId': 'A', 'datasource': PG, 'editorMode': 'code', 'rawQuery': True,
                      'format': 'table', 'rawSql': sql}],
         'fieldConfig': {'defaults': d, 'overrides': overrides or []},
         'options': options or {}}
    if desc:
        p['description'] = desc
    panels.append(p)


KPI_OPTS = {'graphMode': 'none', 'colorMode': 'value', 'textMode': 'value', 'justifyMode': 'center',
            'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}}
row('BI histórico · Supabase en vivo (todas las lecturas del ESP32)', 47)
y = 48
sql_panel('stat', 'Lecturas totales', f'select count(*) as lecturas from {TABLE}', 0, y, 4, 4,
          decimals=0, options=KPI_OPTS)
sql_panel('stat', 'Lecturas válidas', f'select 100.0 * count(*) filter (where {VALID}) / count(*) as validas from {TABLE}',
          4, y, 4, 4, unit='percent', decimals=1, options=KPI_OPTS,
          defaults={'thresholds': {'mode': 'absolute', 'steps': [{'color': 'red', 'value': None},
                                                                  {'color': 'orange', 'value': 80},
                                                                  {'color': 'green', 'value': 95}]}},
          desc='Porcentaje que cumple las reglas de calidad: humedad 0–100 %, gas > 0, temperatura y presión en rango físico.')
sql_panel('stat', 'Días con datos', f'select count(distinct ({LIMA})::date) as dias from {TABLE}', 8, y, 4, 4,
          decimals=0, options=KPI_OPTS)
sql_panel('stat', 'Humedad media histórica (%)', f'select avg(humedad_pct) as humedad from {TABLE} where {VALID}',
          12, y, 4, 4, unit='none', decimals=1, options=KPI_OPTS)
sql_panel('stat', 'Minutos fuera de p5–p95',
          f"with m as (select date_trunc('minute', created_at) mi, avg(humedad_pct) h from {TABLE} where {VALID} group by 1) "
          f'select 100.0 * count(*) filter (where h < {P5} or h > {P95}) / count(*) as fuera from m',
          16, y, 4, 4, unit='percent', decimals=1, options=KPI_OPTS,
          desc=f'Pregunta descriptiva U1: proporción de minutos con humedad media fuera de p5 = {P5} % y p95 = {P95} %.')
sql_panel('stat', 'Última lectura en Supabase (hace)',
          f'select extract(epoch from now() - max(created_at)) as segundos from {TABLE}', 20, y, 4, 4,
          unit='s', decimals=0, options=KPI_OPTS,
          defaults={'thresholds': {'mode': 'absolute', 'steps': [{'color': 'green', 'value': None},
                                                                  {'color': 'orange', 'value': 180},
                                                                  {'color': 'red', 'value': 600}]}},
          desc='Incluye lecturas inválidas: indica si el ESP32 sigue escribiendo en Supabase.')
y += 4
sql_panel('barchart', 'Humedad promedio por hora del día (hora Lima)',
          f"select lpad(extract(hour from {LIMA})::int::text, 2, '0') || ':00' as hora, avg(humedad_pct) as humedad "
          f'from {TABLE} where {VALID} group by 1 order by 1', 0, y, 12, 8, unit='none', decimals=1,
          defaults={'color': {'mode': 'fixed', 'fixedColor': 'green'}},
          options={'xField': 'hora', 'orientation': 'vertical', 'showValue': 'never', 'barWidth': 0.8,
                   'legend': {'showLegend': False}, 'tooltip': {'mode': 'single'}})
sql_panel('timeseries', 'Humedad por día: promedio, mínimo y máximo',
          f"select date_trunc('day', created_at, 'America/Lima') as time, avg(humedad_pct) as promedio, "
          f'min(humedad_pct) as minimo, max(humedad_pct) as maximo from {TABLE} where {VALID} group by 1 order by 1',
          12, y, 12, 8, unit='none', decimals=1,
          defaults={'color': {'mode': 'palette-classic'},
                    'custom': {'lineWidth': 2, 'fillOpacity': 10, 'showPoints': 'always', 'spanNulls': True}},
          overrides=[color('promedio', 'green'), color('minimo', 'blue'), color('maximo', 'orange')],
          options={'legend': {'displayMode': 'list', 'placement': 'bottom', 'showLegend': True},
                   'tooltip': {'mode': 'multi'}})
panels[-1]['timeFrom'] = '60d'
panels[-1]['hideTimeOverride'] = False
y += 8
hours = ',\n'.join(f"round((avg(humedad_pct) filter (where extract(hour from {LIMA}) = {h}))::numeric, 1) as \"{h:02d}\""
                   for h in range(24))
sql_panel('table', 'Mapa de calor: humedad media por día y hora (hora Lima)',
          f"select to_char({LIMA}, 'YYYY-MM-DD') as dia,\n{hours}\nfrom {TABLE} where {VALID} group by 1 order by 1 desc",
          0, y, 24, 10, unit='none', decimals=1,
          defaults={'min': 25, 'max': 50, 'color': {'mode': 'continuous-BlYlRd'},
                    'custom': {'align': 'center', 'cellOptions': {'type': 'color-background', 'mode': 'basic'}}},
          overrides=[{'matcher': {'id': 'byName', 'options': 'dia'},
                      'properties': [{'id': 'custom.cellOptions', 'value': {'type': 'auto'}},
                                     {'id': 'custom.width', 'value': 110}]}],
          options={'showHeader': True, 'cellHeight': 'sm'},
          desc='Azul = más seco, rojo = más húmedo. Celdas vacías: horas sin lecturas válidas.')
y += 10
sql_panel('histogram', 'Distribución de la humedad media por minuto',
          f"select avg(humedad_pct) as humedad from {TABLE} where {VALID} group by date_trunc('minute', created_at)",
          0, y, 12, 8, unit='none',
          defaults={'color': {'mode': 'fixed', 'fixedColor': 'green'}, 'custom': {'fillOpacity': 60, 'lineWidth': 1},
                    'thresholds': {'mode': 'absolute', 'steps': [{'color': 'green', 'value': None},
                                                                  {'color': 'orange', 'value': P5},
                                                                  {'color': 'red', 'value': P95}]}},
          options={'bucketSize': 1, 'combine': False, 'legend': {'showLegend': False}},
          desc=f'Cada barra = 1 punto porcentual. p5 = {P5} % y p95 = {P95} % delimitan el rango normal de U1.')
panels.append({'id': pid(), 'type': 'text', 'title': 'Modelos MLlib comparados (validación cronológica)',
               'gridPos': {'x': 12, 'y': y, 'w': 12, 'h': 8},
               'options': {'mode': 'markdown', 'content': (
                   '| Etapa | Modelo | RMSE val. | Resultado |\n|---|---|---:|---|\n'
                   '| U1 · humedad del minuto | Regresión lineal | 5,668 | |\n'
                   '| U1 · humedad del minuto | Ridge | 5,711 | |\n'
                   '| U1 · humedad del minuto | Elastic Net | 5,737 | |\n'
                   '| U1 · humedad del minuto | **Random Forest** | **4,330** | ganador (no superó la media en test) |\n'
                   '| U2 · t+120 s | **Persistencia** | **0,564** | **en producción** · test RMSE 0,331 |\n'
                   '| U2 · t+120 s | Regresión lineal reg. | 0,624 | |\n'
                   '| U2 · t+120 s | Random Forest | 3,260 | |\n\n'
                   'RMSE en puntos porcentuales de humedad. Selección por validación; el test se evaluó una sola vez.')}})

dashboard = {
    'uid': 'aireiq-vivo', 'title': 'AireIQ · Humedad y predicción real', 'schemaVersion': 39, 'version': 3,
    'tags': ['aireiq', 'humedad', 'kafka', 'spark'], 'timezone': 'browser', 'refresh': '5s',
    'time': {'from': 'now-3h', 'to': 'now'},
    'templating': {'list': [{'name': 'ds', 'label': 'Datasource', 'type': 'datasource', 'query': 'prometheus',
                             'current': {'text': 'Prometheus AireIQ', 'value': 'aireiq-prometheus'},
                             'hide': 0, 'refresh': 1, 'regex': ''}]},
    'panels': panels,
}
text = json.dumps(dashboard, indent=2, ensure_ascii=False) + '\n'
here = Path(__file__).resolve().parents[1]
targets = [here / 'infra/grafana/dashboards/live.json']
team = here.parent / 'grafana/dashboards/aireiq_humedad.json'
if team.parent.is_dir():
    targets.append(team)
for t in targets:
    t.write_text(text, encoding='utf-8')
    print(t)
