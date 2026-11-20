$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    $dockerBin = 'C:\Program Files\Docker\Docker\resources\bin'
    if (-not (Test-Path "$dockerBin\docker.exe")) { $dockerBin = "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin" }
    if (Test-Path "$dockerBin\docker.exe") { $env:PATH = "$dockerBin;$env:PATH" }
    else { throw 'Docker Desktop no esta instalado.' }
}
if (-not (Test-Path .env)) {
    $token = 'sintoken'
    Set-Content .env "JUPYTER_TOKEN=$token" -Encoding ascii
}
New-Item -ItemType Directory -Force outputs | Out-Null
docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Abre Docker Desktop y espera a que el motor este activo.' }
docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Configuracion Compose invalida.' }
docker compose up -d --build --wait --wait-timeout 180
if ($LASTEXITCODE -ne 0) { throw 'No se pudo iniciar Jupyter. Revisa docker compose logs.' }
Write-Host 'Jupyter: http://localhost:4488 - usa el token del archivo local .env'
