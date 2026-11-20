$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    $env:PATH = "C:\Program Files\Docker\Docker\resources\bin;$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
}
docker compose exec -T jupyter jupyter nbconvert --to notebook --execute /opt/aireiq/notebooks/01_entorno_humedad.ipynb --output /opt/aireiq/outputs/01_entorno_humedad_ejecutado.ipynb --ExecutePreprocessor.timeout=180
if ($LASTEXITCODE -ne 0) { throw 'Fallo la ejecucion del cuaderno.' }
