$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle = 'AireIQ - S8 - Spark Structured Streaming'
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s08 | Out-Null
Start-Transcript -Path outputs\s08\demo_visible.log -Force | Out-Null
function Run-Step([string]$label,[string]$command,[string]$log) {
    Write-Host "`n$label" -ForegroundColor Yellow
    Write-Host $command -ForegroundColor Green
    # cmd une stderr/stdout antes de que PowerShell lo capture como texto.
    cmd /c "$command 2>&1" | Tee-Object -FilePath $log
    if($LASTEXITCODE -ne 0){throw "Fallo: $label"}
}
try {
    Write-Host "S8 - CARPETA: $(Get-Location)" -ForegroundColor Cyan
    Run-Step '1. Construir el servicio Spark separado de Jupyter' 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming build spark-streaming' 'outputs/s08/build.log'
    Run-Step '2. Probar duplicados, dato invalido, watermark y checkpoint' 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming run --rm --no-deps spark-streaming test' 'outputs/s08/test.log'
    Run-Step '3. Procesar el historico real que ya esta en Kafka' 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming run --rm --no-deps spark-streaming history' 'outputs/s08/history.log'
    Run-Step '4. Dejar activo el consumidor continuo' 'docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming up -d spark-streaming' 'outputs/s08/start.log'
    Write-Host 'COMPLETADO S8. Revisar manifiestos y logs en outputs.' -ForegroundColor Cyan
    Write-Host 'El watermark depende del tiempo de los eventos, no de esperar dos minutos de reloj.'
} catch { Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red }
finally { Stop-Transcript | Out-Null }
