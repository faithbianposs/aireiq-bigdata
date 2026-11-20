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
stat('Modelo en producción', 'aireiq_live_model_info', 10, 0, w=6, h=6,
     legend='{{modelo}} · horizonte {{horizonte}} · corrida {{corrida}}',
     desc='Modelo MLlib cargado por live_forecast.py. Persistencia ganó la validación cronológica a 120 s.')
panels[-1]['options'].update(textMode='name', graphMode='none', colorMode='none')
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
