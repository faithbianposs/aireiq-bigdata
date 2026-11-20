$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Verificar evento empresarial derivado de prediccion'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\live_forecast | Out-Null
Start-Transcript -Path outputs\live_forecast\verificacion_evento.log -Force | Out-Null
try {
    Write-Host 'Ejecutando verificacion de extremo a extremo para evento empresarial derivado de prediccion...' -ForegroundColor Cyan
    docker cp scripts/verificar_evento_pronostico.py aireiq-eventos-1:/app/verificar_evento_pronostico.py
    docker exec aireiq-eventos-1 python /app/verificar_evento_pronostico.py
    if ($LASTEXITCODE -ne 0) { throw 'Fallo en la verificacion del evento empresarial derivado' }
    Write-Host 'Verificacion completada exitosamente. Manifiesto guardado en outputs/live_forecast/manifest_evento_empresarial.json' -ForegroundColor Green
} finally {
    Stop-Transcript | Out-Null
}
