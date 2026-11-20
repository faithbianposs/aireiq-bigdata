$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - S8 - Verificacion final'
$env:PATH="C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
Start-Transcript -Path outputs\s08\verificacion_visible.log -Force | Out-Null
try {
    Write-Host 'Estado del servicio Spark continuo' -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming ps spark-streaming' -ForegroundColor Green
    docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming ps spark-streaming
    Write-Host 'Ultimos mensajes del servicio' -ForegroundColor Yellow
    Write-Host 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming logs --tail 15 spark-streaming' -ForegroundColor Green
    cmd /c 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming logs --tail 15 spark-streaming 2>&1'
    Write-Host 'Checkpoints confirmados del servicio continuo:' -ForegroundColor Yellow
    Get-ChildItem outputs\s08_live\checkpoint\commits -ErrorAction SilentlyContinue | Where-Object Name -Match '^\d+$' | Select-Object Name,LastWriteTime
    Write-Host 'Manifiestos de pruebas completadas:' -ForegroundColor Yellow
    Get-ChildItem outputs\s08\*\manifest.json,outputs\s08_historico\*\manifest.json | ForEach-Object { Get-Content $_.FullName }
} finally {Stop-Transcript | Out-Null}
