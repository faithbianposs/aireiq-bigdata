$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Pronostico real y Grafana'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\live_forecast | Out-Null
Start-Transcript -Path outputs\live_forecast\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml -f compose.supabase.yaml -f compose.live.yaml --profile live --profile supabase --profile streaming'
try {
 Write-Host 'Generar tablero vivo desde scripts/crear_dashboard_vivo.py'
 & 'C:\Program Files\WindowsApps\OpenAI.CodexPrimaryRuntime.v26-1119-907-0_26.1119.907.0_x64__3k8sg7r9htsxt\dependencies\python\python.exe' scripts/crear_dashboard_vivo.py
 if($LASTEXITCODE -ne 0){throw 'Fallo al generar tablero'}
 foreach($command in @("$compose stop spark-streaming", "$compose build live-forecast", "$compose run --rm --no-deps --entrypoint python live-forecast -m unittest test_live_state -v", "$compose up -d --wait kafka pushgateway telemetry prometheus grafana kafka-ui", "$compose up -d supabase-bridge live-minutes live-forecast")) {
  Write-Host $command -ForegroundColor Green
  cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\live_forecast\ultimo_comando.log
  if($LASTEXITCODE -ne 0){throw 'Fallo en arranque del flujo vivo'}
 }
 Write-Host 'Flujo iniciado. Grafana http://localhost:43000/d/aireiq-vivo' -ForegroundColor Cyan
 Write-Host 'Spark de replay queda detenido para limitar memoria; Jupyter sigue disponible.'
} finally {Stop-Transcript | Out-Null}
