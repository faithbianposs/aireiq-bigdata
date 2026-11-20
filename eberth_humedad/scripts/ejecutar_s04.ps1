$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
docker compose exec -T jupyter jupyter nbconvert --to notebook --execute /opt/aireiq/notebooks/04_modelos_humedad.ipynb --output /opt/aireiq/outputs/04_modelos_humedad_ejecutado.ipynb --ExecutePreprocessor.timeout=600
if ($LASTEXITCODE -ne 0) { throw 'S4 fallo. Revisar el error antes de declarar completada la etapa.' }
