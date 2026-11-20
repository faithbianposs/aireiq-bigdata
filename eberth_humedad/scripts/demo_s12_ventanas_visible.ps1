$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - S12 - Ventanas deslizantes'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;C:\Program Files\Docker\Docker\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s12 | Out-Null
Start-Transcript -Path outputs\s12\ventanas_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming'
function Step([string]$title,[string]$command) {
    Write-Host "`n$title" -ForegroundColor Yellow
    Write-Host $command -ForegroundColor Green
    cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\s12\ultimo_comando.log
    if($LASTEXITCODE -ne 0){throw "Fallo: $title"}
}
try {
    Write-Host "Carpeta: $(Get-Location)"
    Step '1. Construir imagen con prueba deslizante' "$compose build spark-streaming"
    Step '2. Probar ventana de 5 minutos, avance de 1 minuto y watermark de 2 minutos' "$compose run --rm --no-deps spark-streaming test-sliding"
    Write-Host 'COMPLETADO: solapamiento, duplicados, invalidos, tardios y recuperacion verificados.' -ForegroundColor Cyan
} catch { Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red; throw }
finally { Stop-Transcript | Out-Null }
