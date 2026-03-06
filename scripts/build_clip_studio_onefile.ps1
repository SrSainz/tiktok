$ErrorActionPreference = "Stop"

Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "[build-onefile] Installing/Updating PyInstaller..."
python -m pip install --upgrade pyinstaller

Write-Host "[build-onefile] Building ClipStudioES_OneFile.exe ..."
python -m PyInstaller `
  --noconfirm `
  --clean `
  --onefile `
  --windowed `
  --name "ClipStudioES_OneFile" `
  --collect-all yt_dlp `
  --collect-all imageio_ffmpeg `
  --collect-all webvtt `
  scripts/clip_studio_gui.py

Write-Host "[build-onefile] Done."
Write-Host "[build-onefile] EXE: dist\\ClipStudioES_OneFile.exe"
