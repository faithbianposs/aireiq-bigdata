$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Pruebas del puente Supabase'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\supabase | Out-Null
Start-Transcript -Path outputs\supabase\pruebas.log -Force | Out-Null
try {
    $commands=@(
        'docker compose -f compose.yaml -f compose.streaming.yaml -f compose.supabase.yaml --profile supabase build supabase-bridge',
        'docker compose -f compose.yaml -f compose.streaming.yaml -f compose.supabase.yaml --profile supabase run --rm --no-deps --entrypoint python supabase-bridge -m unittest test_supabase_bridge -v'
    )
    foreach($command in $commands) {
        Write-Host $command -ForegroundColor Green
        cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\supabase\ultimo_comando.log
        if($LASTEXITCODE -ne 0){throw 'Fallo en prueba del puente'}
    }
    Write-Host 'PRUEBAS LOCALES COMPLETADAS. No acreditan conexion real a Supabase.' -ForegroundColor Cyan
} finally { Stop-Transcript | Out-Null }
