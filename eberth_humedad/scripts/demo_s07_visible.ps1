$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle = 'AireIQ - S7 - Mediciones IoT en Kafka'
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s07 | Out-Null
Start-Transcript -Path outputs\s07\demo_visible.log -Force | Out-Null
try {
    Write-Host "AIREIQ S7 - REPRODUCCION HISTORICA, NO SENSOR EN VIVO" -ForegroundColor Cyan
    Write-Host "Carpeta: $(Get-Location)"
    Write-Host "`n1. Construir el cliente IoT y comprobar servicios" -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml up -d --build --wait kafka kafka-ui eventos' -ForegroundColor Green
    docker compose -f compose.yaml -f compose.streaming.yaml up -d --build --wait kafka kafka-ui eventos
    if ($LASTEXITCODE -ne 0) { throw 'Fallo de arranque' }
    Write-Host "`n2. Publicar y consumir todo el CSV. Pausa 0: prueba acelerada, no frecuencia real." -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml run --rm --no-deps --entrypoint python eventos iot.py --pausa 0' -ForegroundColor Green
    $ErrorActionPreference = 'Continue'
    docker compose -f compose.yaml -f compose.streaming.yaml run --rm --no-deps --entrypoint python eventos iot.py --pausa 0 2>&1 | Tee-Object -FilePath outputs\s07\ejecucion_cliente.log
    $clientExit = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    if ($clientExit -ne 0) { throw 'Fallo de ingesta IoT; revisar outputs/s07/ejecucion_cliente.log' }
    Write-Host "`n3. Verificar las tres particiones del topic" -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic aireiq-mediciones-v1' -ForegroundColor Green
    docker compose -f compose.yaml -f compose.streaming.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic aireiq-mediciones-v1
    if ($LASTEXITCODE -ne 0) { throw 'Fallo consultando el topic' }
    Write-Host "`nCOMPLETADO S7. Kafka UI: http://localhost:48085" -ForegroundColor Cyan
    Write-Host 'La misma key dispositivo mantiene el orden en una particion. Tener 3 particiones no implica usar las 3 con un solo sensor.'
    Write-Host 'Las lecturas rechazadas quedan documentadas, no se convierten en alertas.'
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
} finally { Stop-Transcript | Out-Null }
