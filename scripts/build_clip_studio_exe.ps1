$ErrorActionPreference = "Stop"

Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "[build] Installing/Updating PyInstaller..."
python -m pip install --upgrade pyinstaller

Write-Host "[build] Building ClipStudioES.exe ..."
python -m PyInstaller `
  --noconfirm `
  --clean `
  --windowed `
  --name "ClipStudioES" `
  --collect-all yt_dlp `
  --collect-all imageio_ffmpeg `
  --collect-all webvtt `
  scripts/clip_studio_gui.py

Write-Host "[build] Done."
Write-Host "[build] EXE: dist\\ClipStudioES\\ClipStudioES.exe"
