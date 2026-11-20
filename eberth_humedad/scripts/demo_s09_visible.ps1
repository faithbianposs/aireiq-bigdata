$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - S9 - Prometheus y Grafana'
$env:PATH="C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s09 | Out-Null
# Credencial local fuera del codigo y sin imprimir su valor en la transcripcion.
if (-not (Select-String -Path .env -Pattern '^GRAFANA_ADMIN_USER=' -Quiet)) { Add-Content .env 'GRAFANA_ADMIN_USER=admin' }
if (-not (Select-String -Path .env -Pattern '^GRAFANA_ADMIN_PASSWORD=' -Quiet)) { Add-Content .env ('GRAFANA_ADMIN_PASSWORD=AireIQ-'+[guid]::NewGuid().ToString('N')) }
Start-Transcript -Path outputs\s09\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml --profile streaming'
function Step([string]$title,[string]$command,[string]$log) {
    Write-Host "`n$title" -ForegroundColor Yellow
    Write-Host $command -ForegroundColor Green
    cmd /c "$command 2>&1" | Tee-Object -FilePath $log
    if($LASTEXITCODE -ne 0){throw "Fallo: $title"}
}
try {
    Write-Host "S9 - Carpeta: $(Get-Location)" -ForegroundColor Cyan
    Step '1. Construir y levantar observabilidad; reiniciar Spark con su checkpoint' "$compose up -d --build --wait --wait-timeout 240 spark-streaming kafka-exporter telemetry prometheus grafana" 'outputs/s09/start.log'
    Step '2. Validar configuracion y reglas de Prometheus' "$compose exec -T prometheus promtool check config /etc/prometheus/prometheus.yml" 'outputs/s09/config-check.log'
    Step '3. Prueba sintetica de la alerta de heartbeat, sin detener el servicio real' "$compose exec -T prometheus promtool test rules /etc/prometheus/test-alerts.yml" 'outputs/s09/alerts-test.log'
    Step '4. Esperar metricas reales y conexion Grafana-Prometheus' "$compose run --rm --no-deps telemetry python verify.py --wait-only" 'outputs/s09/readiness.log'
    Step '5. Reproducir 30 lecturas antiguas: mostrar ingesta y descartes por watermark' "$compose run --rm --no-deps --entrypoint python eventos iot.py --limite 30 --pausa 0.1" 'outputs/s09/traffic.log'
    Step '6. Verificar APIs, consultas del tablero y backlog cero' "$compose run --rm --no-deps telemetry python verify.py" 'outputs/s09/verify.log'
    Step '7. Medir recursos actuales de los contenedores' 'docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"' 'outputs/s09/resources.log'
    Write-Host 'COMPLETADO S9. Grafana: http://localhost:43000/d/aireiq-operacion' -ForegroundColor Cyan
    Write-Host 'Usuario admin; contrasena en .env, campo GRAFANA_ADMIN_PASSWORD.'
    Write-Host 'Prometheus: http://localhost:49090. Las alertas no envian notificaciones externas.'
} catch { Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red }
finally {Stop-Transcript | Out-Null}
