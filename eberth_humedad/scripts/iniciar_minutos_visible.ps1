$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Minutos nuevos con tiempo de disponibilidad'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\live_minutes | Out-Null
Start-Transcript -Path outputs\live_minutes\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml -f compose.supabase.yaml --profile supabase'
try {
 foreach($command in @("$compose build live-minutes", "$compose run --rm --no-deps --entrypoint python live-minutes -m unittest test_minutes -v", "$compose up -d --wait kafka", "$compose up -d supabase-bridge live-minutes")) {
  Write-Host $command -ForegroundColor Green
  cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\live_minutes\ultimo_comando.log
  if($LASTEXITCODE -ne 0){throw 'Fallo en adaptador de minutos'}
 }
 Write-Host 'Adaptador iniciado. Sin eventos recientes no inventa minutos ni pronosticos.' -ForegroundColor Cyan
} finally {Stop-Transcript | Out-Null}
