$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Acceso real Supabase'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\supabase | Out-Null
Start-Transcript -Path outputs\supabase\acceso_real.log -Force | Out-Null
try {
 $command='docker compose -f compose.yaml -f compose.streaming.yaml -f compose.supabase.yaml --profile supabase run --rm --no-deps supabase-bridge --check'
 Write-Host 'Comprobar SELECT en Supabase sin publicar eventos ni mostrar claves.' -ForegroundColor Cyan
 Write-Host $command -ForegroundColor Green
 cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\supabase\acceso_resultado.log
 if($LASTEXITCODE -ne 0){throw 'No se pudo confirmar acceso a Supabase'}
 Write-Host 'Comprobacion de lectura terminada.' -ForegroundColor Cyan
} finally {Stop-Transcript | Out-Null}
