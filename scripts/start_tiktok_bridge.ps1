$ErrorActionPreference = "Stop"

function Stop-BraveProcesses {
  $braveNames = @("brave", "brave-browser")
  foreach ($name in $braveNames) {
    Get-Process -Name $name -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
  }
}

function Stop-ReverseTunnelProcesses {
  $needle = "9223:127.0.0.1:9222"
  Get-CimInstance Win32_Process -Filter "name = 'ssh.exe'" |
    Where-Object { $_.CommandLine -like "*$needle*" } |
    ForEach-Object {
      try {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
      } catch {
        Write-Warning "No pude cerrar el tunel SSH PID $($_.ProcessId): $($_.Exception.Message)"
      }
    }
}

function Wait-HttpReady {
  param(
    [Parameter(Mandatory = $true)][string]$Url,
    [int]$TimeoutSeconds = 20
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    try {
      $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 4
      if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
        return $true
      }
    } catch {
      Start-Sleep -Milliseconds 800
    }
  }
  return $false
}

function Test-NasPort {
  $output = & ssh SergioCloud "ss -tlnp" 2>$null
  if ((($output | Out-String)) -match '[:\]]9223(\s|$)') {
    return "OPEN"
  }
  return "CLOSED"
}

function Wait-NasPort {
  param(
    [int]$TimeoutSeconds = 20
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    if ((Test-NasPort) -like "OPEN*") {
      return $true
    }
    Start-Sleep -Milliseconds 900
  }
  return $false
}

Write-Output "Reiniciando Brave para asegurar CDP en 9222..."
Stop-BraveProcesses
Start-Sleep -Seconds 2

Write-Output "Cerrando tuneles inversos antiguos..."
Stop-ReverseTunnelProcesses

Write-Output "Lanzando Brave con depuracion remota..."
& (Join-Path $PSScriptRoot "start_brave_cdp.ps1") | Out-Host

if (-not (Wait-HttpReady -Url "http://127.0.0.1:9222/json/version" -TimeoutSeconds 25)) {
  throw "Brave no quedo escuchando en http://127.0.0.1:9222/json/version"
}

$sshPath = (Get-Command ssh).Source
$sshArgs = @(
  "-o", "ExitOnForwardFailure=yes",
  "-o", "ServerAliveInterval=30",
  "-o", "ServerAliveCountMax=3",
  "-N",
  "-R", "9223:127.0.0.1:9222",
  "SergioCloud"
)

Write-Output "Abriendo tunel reverse SSH hacia SergioCloud (9223 -> 9222)..."
$proc = Start-Process -FilePath $sshPath -ArgumentList $sshArgs -WindowStyle Hidden -PassThru
Start-Sleep -Seconds 3

if ($proc.HasExited) {
  throw "El tunel SSH salio enseguida. Revisa claves SSH o conectividad con SergioCloud."
}

if (-not (Wait-NasPort -TimeoutSeconds 20)) {
  $remoteStatus = Test-NasPort
  throw "El NAS no ve abierto 127.0.0.1:9223. Respuesta: $remoteStatus"
}

Write-Output "OK: Brave CDP local activo en 9222."
Write-Output "OK: Tunel NAS activo en 127.0.0.1:9223."
Write-Output "PID tunel SSH: $($proc.Id)"
& (Join-Path $PSScriptRoot "start_tiktok_local_uploader.ps1") | Out-Host
