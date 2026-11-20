$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
docker compose exec -T jupyter jupyter nbconvert --to notebook --execute /opt/aireiq/notebooks/03_calidad_humedad.ipynb --output /opt/aireiq/outputs/03_calidad_humedad_ejecutado.ipynb --ExecutePreprocessor.timeout=300
if ($LASTEXITCODE -ne 0) { throw 'S3 fallo. No considerar completada la etapa.' }
