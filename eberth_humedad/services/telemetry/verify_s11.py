import base64,json,os,time,uuid
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.parse import urlencode
def get(url,auth=False):
    headers={}
    if auth:
        credential=os.environ['GRAFANA_ADMIN_USER']+':'+os.environ['GRAFANA_ADMIN_PASSWORD']
        headers['Authorization']='Basic '+base64.b64encode(credential.encode()).decode()
    with urlopen(Request(url,headers=headers),timeout=10) as r:return json.load(r)
deadline=time.monotonic()+120
while time.monotonic()<deadline:
    try:
        dashboard=get('http://grafana:3000/api/dashboards/uid/aireiq-bi',True)
        assert len(dashboard['dashboard']['panels'])>=10
        expressions=['aireiq_bi_observations','aireiq_bi_scored','aireiq_bi_rmse','aireiq_u1_rmse','aireiq_s10_test_rmse','aireiq_bi_humidity_next']
        values={}
        for expression in expressions:
            data=get('http://grafana:3000/api/datasources/proxy/uid/aireiq-prometheus/api/v1/query?'+urlencode({'query':expression}),True)
            assert data['status']=='success' and data['data']['result'],expression
            values[expression]=float(data['data']['result'][0]['value'][1])
        assert values['aireiq_bi_observations']==60 and values['aireiq_bi_scored']>0
        path=Path('/evidence/s11')/('dashboard-'+uuid.uuid4().hex);path.mkdir(parents=True)
        report=dict(status='passed',grafana_dashboard='aireiq-bi',datasource_verified=True,values=values,visual_review=False)
        (path/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report,indent=2));break
    except Exception as exc:
        last=str(exc);time.sleep(5)
else:raise RuntimeError('No se verifico BI: '+last)
