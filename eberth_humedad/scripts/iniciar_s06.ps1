$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
docker compose -f compose.yaml -f compose.streaming.yaml config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose invalido' }
docker compose -f compose.yaml -f compose.streaming.yaml up -d --build --wait --wait-timeout 240 kafka kafka-ui eventos
if ($LASTEXITCODE -ne 0) { throw 'Fallo el arranque S6' }
docker compose -f compose.yaml -f compose.streaming.yaml run --rm --no-deps eventos smoke
if ($LASTEXITCODE -ne 0) { throw 'Fallo la prueba S6' }
