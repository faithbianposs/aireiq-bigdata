$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - S10 - Pronostico de humedad'
$env:PATH="C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s10 | Out-Null
Start-Transcript -Path outputs\s10\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming'
function Step([string]$title,[string]$command,[string]$log) {
    Write-Host "`n$title" -ForegroundColor Yellow;Write-Host $command -ForegroundColor Green
    cmd /c "$command 2>&1" | Tee-Object -FilePath $log
    if($LASTEXITCODE -ne 0){throw "Fallo: $title"}
}
try {
    Write-Host "S10 - Carpeta: $(Get-Location)" -ForegroundColor Cyan
    $env:PYTHONPATH=Join-Path (Get-Location) '.tools\python'
    Write-Host 'Generar el cuaderno S10 desde scripts/crear_s10.py' -ForegroundColor Yellow
    & 'C:\Program Files\WindowsApps\OpenAI.CodexPrimaryRuntime.v26-1119-907-0_26.1119.907.0_x64__3k8sg7r9htsxt\dependencies\python\python.exe' scripts/crear_s10.py
    if($LASTEXITCODE -ne 0){throw 'Fallo al crear cuaderno'}
    Step '1. Construir imagen de inferencia' "$compose build spark-streaming" 'outputs/s10/build.log'
    Step '2. Entrenar y evaluar el cuaderno con datos cronologicos' "$compose run --rm --no-deps --entrypoint jupyter spark-streaming nbconvert --to notebook --execute /notebooks/10_pronostico_humedad.ipynb --output /evidence/10_pronostico_humedad_ejecutado.ipynb --ExecutePreprocessor.timeout=600" 'outputs/s10/train.log'
    Step '3. Probar el modelo guardado con eventos Kafka y Structured Streaming' "$compose run --rm --no-deps --entrypoint spark-submit spark-streaming --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /app/forecast.py" 'outputs/s10/inference.log'
    Write-Host 'COMPLETADO S10. Datos historicos de prueba, no ESP32 en vivo.' -ForegroundColor Cyan
    Write-Host 'Revisa el modelo elegido y si supera persistencia. El resultado no se fuerza.'
} catch {Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red}
finally {Stop-Transcript | Out-Null}
