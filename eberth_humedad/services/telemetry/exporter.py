"""Métricas derivadas del progreso real de Spark, sin valores inventados."""
import json
import math
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATUS=Path('/evidence/s08_live/status.json')
def stamp(s):
    return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()

def metrics():
    lines=[]
    def emit(name,value,help,labels=''):
        value=float(value)
        if not math.isfinite(value):
            return
        if not any(line.startswith('# HELP '+name+' ') for line in lines):
            lines.extend([f'# HELP {name} {help}',f'# TYPE {name} gauge'])
        lines.append(f'{name}{labels} {value}')
    if not STATUS.exists():
        emit('aireiq_status_available',0,'Existe un estado publicado por Spark')
        return '\n'.join(lines)+'\n'
    state=json.loads(STATUS.read_text())
    emit('aireiq_status_available',1,'Existe un estado publicado por Spark')
    age=max(0,time.time()-stamp(state['updated_at']))
    emit('aireiq_heartbeat_age_seconds',age,'Segundos desde la actualizacion del proceso Spark')
    emit('aireiq_stream_active',int(state.get('active',False) and age<=30),'Consulta activa con heartbeat reciente')
    p=state.get('progress') or {}
    d=state.get('last_data_batch') or {}
    if p:
        emit('aireiq_batch_id',p['batchId'],'Ultimo microbatch reportado')
        emit('aireiq_progress_age_seconds',max(0,time.time()-stamp(p['timestamp'])),'Antiguedad del progreso de microbatch')
        for src in p.get('sources',[]):
            end=src.get('endOffset')
            if isinstance(end,str):
                end=json.loads(end)
            if isinstance(end,dict):
                for topic, offsets in end.items():
                    for partition, value in offsets.items():
                        labels='{topic='+json.dumps(topic)+',partition='+json.dumps(str(partition))+'}'
                        emit('aireiq_spark_end_offset',value,'Offset final procesado por Spark',labels)
        watermark=p.get('eventTime',{}).get('watermark')
        if watermark:
            emit('aireiq_watermark_timestamp_seconds',stamp(watermark),'Watermark como timestamp Unix de tiempo de evento')
        emit('aireiq_state_memory_bytes',sum(s.get('memoryUsedBytes',0) for s in p.get('stateOperators',[])),'Memoria de los operadores de estado')
        emit('aireiq_state_rows',sum(s.get('numRowsTotal',0) for s in p.get('stateOperators',[])),'Filas de estado sumadas entre operadores, no eventos unicos')
    if d:
        emit('aireiq_last_data_batch_rows',d['numInputRows'],'Entradas del ultimo microbatch no vacio')
        emit('aireiq_last_data_batch_age_seconds',max(0,time.time()-stamp(d['timestamp'])),'Antiguedad del ultimo microbatch con datos')
        emit('aireiq_last_data_batch_duration_seconds',d.get('durationMs',{}).get('triggerExecution',0)/1000,'Duracion de ejecucion del ultimo microbatch con datos, no latencia extremo a extremo')
        emit('aireiq_last_data_batch_processed_rows_per_second',d.get('processedRowsPerSecond',0),'Rendimiento del ultimo microbatch con datos')
        emit('aireiq_last_data_batch_dropped_watermark',sum(s.get('numRowsDroppedByWatermark',0) for s in d.get('stateOperators',[])),'Descartes por watermark en el ultimo microbatch con datos')
    return '\n'.join(lines)+'\n'

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path!='/metrics':
            self.send_error(404); return
        try:
            data=metrics().encode()
        except (ValueError,KeyError,OSError) as exc:
            self.send_error(503,'Estado no disponible'); return
        self.send_response(200)
        self.send_header('Content-Type','text/plain; version=0.0.4; charset=utf-8')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers(); self.wfile.write(data)
    def log_message(self,*args):
        pass

if __name__=='__main__':
    ThreadingHTTPServer(('0.0.0.0',9108),Handler).serve_forever()
