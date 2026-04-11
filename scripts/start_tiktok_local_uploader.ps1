$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDir ".."))
$python = (Get-Command python).Source
$serviceScript = Join-Path $scriptDir "tiktok_local_uploader_service.py"
$logDir = Join-Path $repoRoot "data"
$stdoutLog = Join-Path $logDir "tiktok_local_uploader.out.log"
$stderrLog = Join-Path $logDir "tiktok_local_uploader.err.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$existing = Get-CimInstance Win32_Process -Filter "name = 'python.exe'" |
  Where-Object { $_.CommandLine -like "*tiktok_local_uploader_service.py*" }
if ($existing) {
  Write-Output "Uploader local ya activo."
  return
}

Start-Process -FilePath $python -ArgumentList @($serviceScript) -WorkingDirectory $repoRoot -WindowStyle Hidden -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog | Out-Null
Write-Output "Uploader local TikTok iniciado en http://0.0.0.0:8766"
