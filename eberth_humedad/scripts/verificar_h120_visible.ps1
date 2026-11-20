$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Inferencia Kafka horizonte 120 segundos'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s10_h120_streaming | Out-Null
Start-Transcript -Path outputs\s10_h120_streaming\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming'
try {
 foreach($command in @("$compose up -d --wait kafka", "$compose stop jupyter spark-streaming", "$compose build spark-streaming", "$compose run --rm --no-deps -e FORECAST_HORIZON_SECONDS=120 --entrypoint spark-submit spark-streaming --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /app/forecast.py")) {
  Write-Host $command -ForegroundColor Green
  cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\s10_h120_streaming\ultimo_comando.log
  if($LASTEXITCODE -ne 0){throw 'Fallo en verificacion de inferencia'}
 }
} finally {
 & docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming start jupyter spark-streaming
 Stop-Transcript | Out-Null
}
