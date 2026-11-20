$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
docker compose exec -T jupyter jupyter nbconvert --to notebook --execute /opt/aireiq/notebooks/05_producto_u1_humedad.ipynb --output /opt/aireiq/outputs/05_producto_u1_humedad_ejecutado.ipynb --ExecutePreprocessor.timeout=600
if ($LASTEXITCODE -ne 0) { throw 'S5 no termino correctamente. No presentar como verificada.' }
