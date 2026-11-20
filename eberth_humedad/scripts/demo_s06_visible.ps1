$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle = 'AireIQ - S6 Kafka - Demostracion'
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s06 | Out-Null
$log = Join-Path (Get-Location) 'outputs\s06\demo_visible.log'
Start-Transcript -Path $log -Force | Out-Null
try {
    Write-Host "`nAIREIQ - S6: EVENTOS EMPRESARIALES CON KAFKA" -ForegroundColor Cyan
    Write-Host "Carpeta actual: $(Get-Location)"
    Write-Host "`n1. Ejecutamos el mismo script de arranque y prueba:" -ForegroundColor Yellow
    Write-Host '.\scripts\iniciar_s06.ps1' -ForegroundColor Green
    Write-Host 'Valida Compose, inicia servicios y publica 3 eventos simulados y 1 invalido.'
    Write-Host 'Si ya estan encendidos, Docker reutiliza los servicios existentes.'
    & .\scripts\iniciar_s06.ps1
    Write-Host "`n2. Consultamos los contenedores:" -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml ps' -ForegroundColor Green
    docker compose -f compose.yaml -f compose.streaming.yaml ps
    if ($LASTEXITCODE -ne 0) { throw 'Fallo consulta de contenedores' }
    Write-Host "`n3. Consultamos offsets y LAG del grupo consumidor:" -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group aireiq-auditoria-v1' -ForegroundColor Green
    docker compose -f compose.yaml -f compose.streaming.yaml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group aireiq-auditoria-v1
    if ($LASTEXITCODE -ne 0) { throw 'Fallo consulta de offsets' }
    Write-Host "`nCOMPLETADO. LAG=0 significa que no quedan eventos pendientes en ese momento." -ForegroundColor Cyan
    Write-Host 'Kafka UI: http://localhost:48085'
    Write-Host 'JupyterLab: http://localhost:4488/lab'
    Write-Host 'Los comandos verdes muestran lo ejecutado. Puedes subir para revisar la salida.'
    Write-Host 'Esta ventana permanece abierta; cerrarla no apaga los contenedores.'
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    Stop-Transcript | Out-Null
}
