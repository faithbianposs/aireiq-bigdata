$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$Host.UI.RawUI.WindowTitle='AireIQ - Verificar prediccion y error real'
Start-Transcript -Path outputs\live_forecast\verificacion_visible.log -Force | Out-Null
try {
 $settings=@{}
 foreach($line in Get-Content .env){if($line -match '^([^#=]+)=(.*)$'){$settings[$matches[1]]=$matches[2]}}
 $credentials=$settings['GRAFANA_ADMIN_USER']+':'+$settings['GRAFANA_ADMIN_PASSWORD']
 $headers=@{Authorization='Basic '+[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($credentials))}
 $deadline=(Get-Date).AddSeconds(300)
 $values=@{}; $lastError=''; $complete=$false
 Write-Host 'Consultar tablero aireiq-vivo y metricas mediante el datasource de Grafana. No se muestran credenciales.' -ForegroundColor Cyan
 while((Get-Date) -lt $deadline){
  try {
   $dash=Invoke-RestMethod 'http://localhost:43000/api/dashboards/uid/aireiq-vivo' -Headers $headers -TimeoutSec 10
   if($dash.dashboard.panels.Count -lt 10){throw 'Faltan paneles'}
   $values=@{}
   foreach($metric in @('observations','timely_predictions','late_predictions','scored','rmse','mae','baseline_rmse','lead_seconds','active','updated_timestamp')) {
    $url='http://localhost:43000/api/datasources/proxy/uid/aireiq-prometheus/api/v1/query?query=aireiq_live_'+$metric
    $response=Invoke-RestMethod $url -Headers $headers -TimeoutSec 10
    if($response.data.result.Count -gt 0){$values[$metric]=[double]::Parse($response.data.result[0].value[1],[Globalization.CultureInfo]::InvariantCulture)}
   }
   $fresh=([DateTimeOffset]::UtcNow.ToUnixTimeSeconds()-$values['updated_timestamp']) -lt 30
   if($values['timely_predictions'] -gt 0 -and $values['scored'] -gt 0 -and $values['active'] -eq 1 -and $fresh){$complete=$true;break}
   Write-Host ("Esperando pares reales: observaciones={0}, oportunas={1}, evaluadas={2}" -f $values['observations'],$values['timely_predictions'],$values['scored'])
  } catch { $lastError=$_.Exception.GetType().Name; Write-Host "Esperando servicios: $lastError" }
  Start-Sleep -Seconds 10
 }
 $status=if($complete){'passed'}else{'pending_live_evidence'}
 $report=@{status=$status;values=$values;dashboard='aireiq-vivo';visual_review=$false;last_error_type=$lastError;checked_at=[DateTimeOffset]::UtcNow.ToString('o')}
 $report | ConvertTo-Json -Depth 6 | Set-Content -Encoding utf8 outputs\live_forecast\verification.json
 $report | ConvertTo-Json -Depth 6 | Write-Host
} finally {Stop-Transcript | Out-Null}
