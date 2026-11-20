$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Validacion horizonte 120 segundos'
$env:PATH="$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
New-Item -ItemType Directory -Force outputs\s10_h120 | Out-Null
Start-Transcript -Path outputs\s10_h120\demo_visible.log -Force | Out-Null
$compose='docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming'
function Step([string]$command) {
 Write-Host $command -ForegroundColor Green
 cmd /c "$command 2>&1" | Tee-Object -FilePath outputs\s10_h120\ultimo_comando.log
 if($LASTEXITCODE -ne 0){throw 'Fallo en entrenamiento; revisar salida'}
}
try {
 $env:PYTHONPATH=Join-Path (Get-Location) '.tools\python'
 Write-Host 'Generar experimento independiente: scripts/crear_s10_h120.py'
 & 'C:\Program Files\WindowsApps\OpenAI.CodexPrimaryRuntime.v26-1119-907-0_26.1119.907.0_x64__3k8sg7r9htsxt\dependencies\python\python.exe' scripts/crear_s10_h120.py
 if($LASTEXITCODE -ne 0){throw 'Fallo al generar cuaderno'}
 Write-Host 'Pausa temporal de kernels Jupyter y Spark de fondo para liberar memoria.' -ForegroundColor Yellow
 Step "$compose stop jupyter spark-streaming"
 Step "$compose run --rm --no-deps --entrypoint jupyter spark-streaming nbconvert --to notebook --execute /notebooks/10_pronostico_humedad_120s.ipynb --output /evidence/10_pronostico_humedad_120s_ejecutado.ipynb --ExecutePreprocessor.timeout=600"
 Write-Host 'Entrenamiento y evaluacion completados; no se presupone mejora.' -ForegroundColor Cyan
} finally {
 & docker compose -f compose.yaml -f compose.streaming.yaml --profile streaming start jupyter spark-streaming
 Stop-Transcript | Out-Null
}
