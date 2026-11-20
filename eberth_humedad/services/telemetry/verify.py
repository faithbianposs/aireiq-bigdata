import base64
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode

def get(url,auth=False):
    headers={}
    if auth:
        credential=os.environ['GRAFANA_ADMIN_USER']+':'+os.environ['GRAFANA_ADMIN_PASSWORD']
        headers['Authorization']='Basic '+base64.b64encode(credential.encode()).decode()
    with urlopen(Request(url,headers=headers),timeout=10) as response:
        return json.load(response)

def query(expr):
    result=get('http://prometheus:9090/api/v1/query?'+urlencode({'query':expr}))
    assert result['status']=='success'
    return result['data']['result']

deadline=time.monotonic()+180
last='sin respuesta'
while time.monotonic()<deadline:
    try:
        targets=get('http://prometheus:9090/api/v1/targets')['data']['activeTargets']
        assert {t['labels']['job'] for t in targets if t['health']=='up'} >= {'aireiq','kafka','prometheus'}
        active=query('aireiq_stream_active')
        assert active and float(active[0]['value'][1])==1
        dashboard=get('http://grafana:3000/api/dashboards/uid/aireiq-operacion',auth=True)
        assert len(dashboard['dashboard']['panels'])==13
        proxy=get('http://grafana:3000/api/datasources/proxy/uid/aireiq-prometheus/api/v1/query?'+urlencode({'query':'aireiq_stream_active'}),auth=True)
        assert proxy['status']=='success' and proxy['data']['result']
        if '--wait-only' in sys.argv:
            print('Servicios, Spark y datasource Grafana listos.'); sys.exit(0)
        rows=query('aireiq_last_data_batch_rows')
        assert rows and float(rows[0]['value'][1])>0
        lag=query('sum(clamp_min(kafka_topic_partition_current_offset{topic="aireiq-mediciones-v1"} - on(topic,partition) aireiq_spark_end_offset,0))')
        assert lag and float(lag[0]['value'][1])==0
        state=query('aireiq_state_memory_bytes'); assert state
        rules=get('http://prometheus:9090/api/v1/rules')['data']['groups']
        assert sum(len(g['rules']) for g in rules)>=3
        report=dict(status='passed',completed_at=datetime.now(timezone.utc).isoformat(),
                    targets=[{'job':t['labels']['job'],'health':t['health']} for t in targets],
                    grafana_dashboard=dashboard['dashboard']['uid'],panels=13,datasource_query_verified=True,
                    spark_active=active,lag=lag,last_data_rows=rows,state_memory=state,
                    alerts_loaded=sum(len(g['rules']) for g in rules),
                    visual_review=False,external_notifications=False)
        out=Path('/evidence/s09')/str(uuid.uuid4());out.mkdir(parents=True)
        (out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report,indent=2)); break
    except Exception as exc:
        last=type(exc).__name__+': '+str(exc)
        time.sleep(5)
else:
    raise RuntimeError('No se verifico observabilidad: '+last)
