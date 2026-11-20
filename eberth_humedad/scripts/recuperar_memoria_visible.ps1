param([switch]$Reanudar)
$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Recuperacion de memoria'
$docker=Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
if ($Reanudar) {
    Write-Host 'Reanudar Jupyter y Spark conservando sus contenedores y checkpoints.' -ForegroundColor Cyan
    & $docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming start jupyter spark-streaming
} else {
    Write-Host 'Detener temporalmente Jupyter y Spark de fondo. Los archivos y checkpoints se conservan; los kernels se detienen.' -ForegroundColor Yellow
    & $docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming stop jupyter spark-streaming
}
Write-Host "Codigo de salida: $LASTEXITCODE"
