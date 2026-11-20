$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - S11 - BI y error de pronostico'
$env:PATH="C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s11 | Out-Null
Start-Transcript -Path outputs\s11\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml --profile streaming'
function Step([string]$title,[string]$command,[string]$log) {
    Write-Host "`n$title" -ForegroundColor Yellow;Write-Host $command -ForegroundColor Green
    cmd /c "$command 2>&1" | Tee-Object -FilePath $log
    if($LASTEXITCODE -ne 0){throw "Fallo: $title"}
}
try {
    Write-Host "S11 - Carpeta: $(Get-Location)" -ForegroundColor Cyan
    Step '1. Preparar Pushgateway y verificador de BI' "$compose up -d --build --wait pushgateway telemetry" 'outputs/s11/start.log'
    Step '2. Cargar el nuevo scrape de Pushgateway y tablero Grafana' "$compose restart prometheus grafana" 'outputs/s11/reload.log'
    Step '3. Construir la imagen con el evaluador de pronosticos' "$compose build spark-streaming" 'outputs/s11/build.log'
    Step '4. Reproducir 60 minutos y evaluar al llegar cada observacion' "$compose run --rm --no-deps --entrypoint spark-submit spark-streaming --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /app/bi.py" 'outputs/s11/bi.log'
    Step '5. Verificar los datos del tablero mediante Grafana y Prometheus' "$compose run --rm --no-deps telemetry python verify_s11.py" 'outputs/s11/verify.log'
    Write-Host 'COMPLETADO S11. Grafana: http://localhost:43000/d/aireiq-bi' -ForegroundColor Cyan
    Write-Host 'La demostracion termina; sus metricas permanecen con estado activo=0.'
} catch {Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red}
finally {Stop-Transcript | Out-Null}
