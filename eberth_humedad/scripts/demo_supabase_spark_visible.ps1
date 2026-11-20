$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Supabase Kafka Spark Parquet'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\supabase_spark | Out-Null
Start-Transcript -Path outputs\supabase_spark\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml -f compose.supabase.yaml --profile streaming --profile supabase'
function Step([string]$command) {
 Write-Host $command -ForegroundColor Green
 cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\supabase_spark\ultimo_comando.log
 if($LASTEXITCODE -ne 0){throw 'Fallo; revisar el comando anterior'}
}
try {
 Step "$compose up -d --wait kafka"
 Write-Host 'Pausa de Jupyter y Spark de fondo para evitar falta de memoria. Los kernels se detienen; archivos y checkpoints se conservan.' -ForegroundColor Yellow
 Step "$compose stop jupyter spark-streaming"
 Step "$compose build spark-streaming"
 Step "$compose run --rm --no-deps spark-streaming supabase-history"
 Write-Host 'PRUEBA COMPLETADA. Procesamiento hasta los offsets disponibles al iniciar; no es inferencia continua.' -ForegroundColor Cyan
} finally {
 Write-Host 'Reanudando servicios pausados.' -ForegroundColor Yellow
 & docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming start jupyter spark-streaming
 Stop-Transcript | Out-Null
}
