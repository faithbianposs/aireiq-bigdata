import json
from pathlib import Path
panels=[dict(id=1,type='text',title='Humedad: flujo real',gridPos=dict(x=0,y=0,w=24,h=4),
 options=dict(mode='markdown',content='**Supabase → Kafka → minutos nuevos → Spark MLlib → Eventos de negocio (cooldown) → Kafka auditoría.** Horizonte: inicio del minuto de entrada +120 s; no son 120 s desde la emisión. Solo cuentan como anticipación las predicciones emitidas antes de comenzar el minuto objetivo. Los agregados pueden estar incompletos. Sin observación objetivo no hay error; sin muestras, el panel queda vacío. Persistencia fue seleccionada. Eventos empresariales derivados con cooldown emitidos a `aireiq-intervenciones-v1` y auditados en `aireiq-intervenciones-auditadas-v1`.'))]
items=[('Humedad observada','humidity_actual','percent'),('Última predicción oportuna','humidity_prediction','percent'),
 ('RMSE histórico de prueba (120 s)','batch_test_rmse','none'),('RMSE vivo: solo pares oportunos','rmse','none'),
 ('MAE vivo','mae','none'),('Pares evaluados','scored','short'),('Predicciones oportunas','timely_predictions','short'),
 ('Resultados tardíos excluidos','late_predictions','short'),('Margen antes del inicio objetivo (s)','lead_seconds','s'),
 ('Lecturas del último minuto','readings_in_minute','short'),('Servicio activo','active','short'),
 ('Antigüedad del último dato (s)','age','s')]
for i,(title,name,unit) in enumerate(items):
 expr='time() - aireiq_live_event_timestamp' if name=='age' else 'aireiq_live_'+name
 panels.append(dict(id=i+2,type='stat',title=title,gridPos=dict(x=(i%4)*6,y=4+(i//4)*4,w=6,h=4),
  datasource=dict(type='prometheus',uid='aireiq-prometheus'),targets=[dict(refId='A',expr=expr,instant=True)],
  fieldConfig=dict(defaults=dict(unit=unit),overrides=[]),options=dict(reduceOptions=dict(calcs=['lastNotNull'],fields='',values=False))))
panels.append(dict(id=99,type='stat',title='Antigüedad de actualización del servicio (s)',gridPos=dict(x=0,y=16,w=12,h=4),
 datasource=dict(type='prometheus',uid='aireiq-prometheus'),targets=[dict(refId='A',expr='time() - aireiq_live_updated_timestamp',instant=True)],
 fieldConfig=dict(defaults=dict(unit='s'),overrides=[])))
panels.append(dict(id=100,type='stat',title='Inicio objetivo de la última predicción oportuna',gridPos=dict(x=12,y=16,w=12,h=4),
 datasource=dict(type='prometheus',uid='aireiq-prometheus'),targets=[dict(refId='A',expr='aireiq_live_target_timestamp * 1000',instant=True)],
 fieldConfig=dict(defaults=dict(unit='dateTimeAsIso'),overrides=[])))
panels.append(dict(id=101,type='stat',title='Intervenciones de negocio emitidas',gridPos=dict(x=0,y=20,w=12,h=4),
 datasource=dict(type='prometheus',uid='aireiq-prometheus'),targets=[dict(refId='A',expr='aireiq_live_interventions_emitted',instant=True)],
 fieldConfig=dict(defaults=dict(unit='short'),overrides=[])))
panels.append(dict(id=102,type='stat',title='Alertas suprimidas por cooldown',gridPos=dict(x=12,y=20,w=12,h=4),
 datasource=dict(type='prometheus',uid='aireiq-prometheus'),targets=[dict(refId='A',expr='aireiq_live_interventions_suppressed',instant=True)],
 fieldConfig=dict(defaults=dict(unit='short'),overrides=[])))
dashboard=dict(uid='aireiq-vivo',title='AireIQ · Humedad y predicción real',schemaVersion=39,version=2,
 timezone='America/Lima',refresh='5s',time=dict(**{'from':'now-30m','to':'now'}),panels=panels)
target=Path(__file__).resolve().parents[1]/'infra/grafana/dashboards/live.json'
target.write_text(json.dumps(dashboard,indent=2,ensure_ascii=False),encoding='utf-8')
print(target)
