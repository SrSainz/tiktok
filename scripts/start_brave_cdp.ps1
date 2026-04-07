$ErrorActionPreference = "Stop"

$brave = Join-Path ${env:ProgramFiles} "BraveSoftware\Brave-Browser\Application\brave.exe"
if (-not (Test-Path $brave)) {
  throw "No se encontro Brave en $brave"
}

$ip = Get-NetIPAddress -AddressFamily IPv4 |
  Where-Object {
    $_.IPAddress -notlike '169.254*' -and
    $_.InterfaceAlias -notmatch 'Loopback|vEthernet' -and
    $_.PrefixOrigin -ne 'WellKnown'
  } |
  Sort-Object InterfaceMetric, SkipAsSource |
  Select-Object -First 1 -ExpandProperty IPAddress

if (-not $ip) {
  throw "No se pudo detectar una IPv4 local."
}

$args = @(
  "--remote-debugging-address=0.0.0.0"
  "--remote-debugging-port=9222"
  "--profile-directory=Default"
  "https://www.tiktok.com/upload?lang=es"
)

Start-Process -FilePath $brave -ArgumentList $args | Out-Null

Write-Output "Brave lanzado con CDP en: http://$ip`:9222"
Write-Output "Si Windows Firewall pregunta, permite el acceso en la red privada."
