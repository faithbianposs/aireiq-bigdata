$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Supabase a Kafka real'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\supabase | Out-Null
Start-Transcript -Path outputs\supabase\integracion_real.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.observability.yaml -f compose.supabase.yaml --profile supabase'
try {
 $commands=@(
   "$compose build supabase-bridge",
   "$compose up -d --wait kafka",
   "$compose run --rm --no-deps --entrypoint python supabase-bridge verify_supabase_kafka.py",
   "$compose up -d supabase-bridge"
 )
 foreach($command in $commands) {
   Write-Host $command -ForegroundColor Green
   cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\supabase\integracion_comando.log
   if($LASTEXITCODE -ne 0){throw 'Fallo en integracion Supabase; revisar salida anterior'}
 }
 Write-Host 'Prueba aprobada y puente continuo iniciado. El primer recorrido incluye historico.' -ForegroundColor Cyan
} finally {Stop-Transcript | Out-Null}
