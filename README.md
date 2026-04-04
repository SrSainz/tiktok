# YouTube -> TikTok Pipeline (ES)

Automatiza este flujo:

1. Buscar videos de YouTube (canales de Espana, busqueda o URL directa).
2. Elegir candidatos con mas vistas.
3. Descargar un video fuente.
4. Detectar un tramo de alto impacto (por densidad de subtitulos, uso interno).
5. Generar un short vertical limpio (sin subtitulos ni texto superpuesto).
6. Guardarlo en `output/` o subirlo a TikTok (opcional).

## Requisitos

- Python 3.11+ (probado con Python 3.13).
- Dependencias Python:

```bash
pip install -r requirements.txt
```

- Para subida automatica:

```bash
playwright install chromium
```

## Configuracion de canales (Espana)

Edita `channels_es.txt` con los canales que quieras escanear.

## Uso rapido

### 1) Solo ver candidatos (sin descargar ni editar)

```bash
python scripts/youtube_tiktok_pipeline.py --mode channels --dry-run
```

### 2) Generar un corto desde canales de Espana (60s)

```bash
python scripts/youtube_tiktok_pipeline.py --mode channels --duration 60 --max-results 5
```

### 2b) Solo videos de esta semana y orden viral (vistas/dia)

```bash
python scripts/youtube_tiktok_pipeline.py --mode channels --this-week-only --sort-by viral --duration 60 --max-results 10 --dry-run
```

### 3) Generar desde busqueda

```bash
python scripts/youtube_tiktok_pipeline.py --mode search --query "curiosidades ciencia espana" --duration 60
```

### 4) Generar desde URL concreta

```bash
python scripts/youtube_tiktok_pipeline.py --mode url --url "https://www.youtube.com/watch?v=VIDEO_ID" --duration 60
```

## Subida a TikTok (opcional)

### Carga asistida (revisas y publicas manualmente)

```bash
python scripts/youtube_tiktok_pipeline.py --mode channels --duration 60 --publish-tiktok --tiktok-profile-dir .tiktok_profile
```

La primera vez:

1. Se abrira Chromium.
2. Inicia sesion en TikTok.
3. Deja que el script suba el video y complete caption.
4. Publica manualmente desde la ventana.

### Intento de publicacion automatica

```bash
python scripts/youtube_tiktok_pipeline.py --mode channels --publish-tiktok --tiktok-auto-post
```

Nota: TikTok cambia su interfaz con frecuencia. El modo auto-post es best effort.

### Subir un MP4 ya generado (sin reprocesar)

```bash
python scripts/upload_to_tiktok.py --latest-from output --profile-dir .tiktok_profile --caption "Tu caption #tiktok #viral"
```

Si Google bloquea login, usa Chrome normal (ya por defecto):

```bash
python scripts/upload_to_tiktok.py --latest-from output --profile-dir .tiktok_profile --browser-channel chrome
```

Modo robusto (reusar tu perfil real de Chrome):

```bash
python scripts/upload_to_tiktok.py --latest-from output --browser-channel chrome --use-system-chrome-profile --chrome-profile-directory Default
```

Importante: cierra Chrome antes de ejecutar ese comando.

Si prefieres Brave:

```bash
python scripts/upload_to_tiktok.py --latest-from output --browser-channel brave --brave-profile-directory Default
```

Importante: cierra Brave antes de ejecutar ese comando.

### Subida oficial por API (OAuth + Direct Post)

Configura variables de entorno:

```powershell
$env:TIKTOK_CLIENT_KEY="TU_CLIENT_KEY"
$env:TIKTOK_CLIENT_SECRET="TU_CLIENT_SECRET"
$env:TIKTOK_REDIRECT_URI="http://127.0.0.1:8765/callback/"
```

1) OAuth (obtiene `access_token`):

```bash
python scripts/tiktok_direct_post_api.py auth --scopes "video.upload,video.publish" --token-out .tiktok_tokens.json
```

2) Direct Post completo (`POST init` + `PUT chunks` + `POST status`):

```bash
python scripts/tiktok_direct_post_api.py post --token-file .tiktok_tokens.json --video output\\mi_clip.mp4 --title "Prueba API Direct Post #clip" --privacy-level SELF_ONLY
```

Si solo quieres el flujo `video.upload` (sin publicacion directa):

```bash
python scripts/tiktok_direct_post_api.py upload-only --token-file .tiktok_tokens.json --video output\\mi_clip.mp4 --check-status
```

Nota: en la API actual, el titulo y privacidad se envian en el `POST /v2/post/publish/video/init/` (Direct Post).

## Salidas

- Video final: `output/*_tiktok.mp4`
- Metadatos del clip: `output/*_tiktok.json`
- Archivos temporales: `work/`

## Dashboard de opciones (subida manual)

Genera varias opciones de clip desde una URL de YouTube, con preview y ranking:

```bash
python scripts/clip_dashboard.py --url "https://www.youtube.com/watch?v=VIDEO_ID" --duration 60 --options 6
```

Te deja una carpeta como:

- `output/clip_dashboard_<slug>/dashboard.html`
- `output/clip_dashboard_<slug>/options_manifest.json`
- `output/clip_dashboard_<slug>/option_01.mp4` ... `option_06.mp4`

Abres `dashboard.html`, eliges la opción y subes manualmente ese `.mp4` a TikTok.

Cada opcion incluye:

- `Interes` (enganche por ritmo/contenido)
- `Alcance` (potencial de retencion/compartido)
- `Audio` (energia de voz/sonido en ese tramo)
- `Visual` (ritmo de cambios de escena)
- `Descripcion breve` del momento del clip

Ranking global aproximado:

- `SCORE = 46% INTEREST + 33% REACH + 12% AUDIO + 9% VISUAL`
- Se aplica penalizacion para evitar opciones demasiado parecidas tematicamente.

## App de escritorio (GUI)

Lanza interfaz visual:

```bash
python scripts/clip_studio_gui.py
```

En la GUI ya no necesitas pegar URL todo el rato:

1. Pulsa `Buscar virales` (creadores ES: TheGrefg, AuronPlay, Ibai, YoSoyPlex, elrubius).
2. Doble click en el candidato que quieras.
3. Pulsa `Generar Opciones` para crear los cortes.
4. Al terminar, se abre `dashboard.html` automaticamente (puedes desmarcarlo en la GUI).

Si defines `YOUTUBE_API_KEY`, la deteccion usa primero la YouTube Data API oficial con `regionCode=ES`
y categorias de tendencia (`20,24` por defecto) y solo cae a scrape/fallback si falla.

Flujo automatico estilo Opus:

1. Pulsa `Auto IA (buscar + generar)`.
2. La app puntua candidatos por `AI Score` (vistas/dia, frescura, gancho del titulo, duracion ideal).
3. Selecciona el mejor video automaticamente y genera opciones de clips.

## Generar EXE (Windows)

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build_clip_studio_exe.ps1
```

Resultado:

- `dist/ClipStudioES/ClipStudioES.exe`

## Version web (Vercel + Railway)

Arquitectura recomendada:

- Frontend en Vercel (archivo `index.html`).
- Backend de procesamiento en Railway (`backend/app.py`).

### 1) Deploy backend en Railway

Desde este mismo repo:

- Start command: `uvicorn backend.app:app --host 0.0.0.0 --port $PORT`
- Variables:
  - `CORS_ORIGINS=https://TU_FRONTEND.vercel.app`
  - `BACKEND_PUBLIC_URL=https://TU_BACKEND.up.railway.app`
  - `OUTPUT_DIR=/data/output` (opcional)
  - `WORK_DIR=/data/work` (opcional)
  - `YOUTUBE_API_KEY=...` (recomendado para descubrir virales ES con la API oficial)
  - `YOUTUBE_TREND_CATEGORY_IDS=20,24` (opcional; gaming + entretenimiento por defecto)
  - `YTDLP_COOKIES_FILE=/data/cookies.txt` (recomendado para evitar bloqueos "not a bot" de YouTube)
  - `YTDLP_COOKIES_TEXT=<contenido completo de cookies.txt>` (alternativa sin subir archivo)

Endpoints backend:

- `GET /api/health`
- `POST /api/discover`
- `POST /api/jobs`
- `GET /api/jobs/{job_id}`
- `GET /output/...` (clips generados)

Nota YouTube anti-bot:

- Si el job falla con `Sign in to confirm you're not a bot`, el backend necesita cookies de YouTube.
- Exporta `cookies.txt` (formato Netscape) desde tu navegador y súbelo al servidor en `/data/cookies.txt`.
- Si no puedes subir archivos al volumen, pega ese contenido en `YTDLP_COOKIES_TEXT`.
- Verifica en `GET /api/health`:
  - `"cookies_configured": true`
  - `"cookies_file_exists": true`
  - `"youtube_api_configured": true` si quieres usar charts oficiales ES

### 2) Deploy frontend en Vercel

Ya incluido en el repo (`vercel.json` + `index.html`).

En la web:

1. Pega la URL del backend (Railway) en `Backend URL`.
2. Pulsa `Probar conexion`.

### Vercel como proxy hacia el NAS

Si quieres mantener `Vercel` como interfaz publica y usar el `NAS` en vez de Railway:

- El frontend sigue en Vercel.
- Las rutas `GET/POST /api/*` y `GET /output/*` se proxifican al backend del NAS por `SSH`.

Variables necesarias en Vercel:

- `NAS_SSH_HOST=nas.polysainz.com`
- `NAS_SSH_PORT=22`
- `NAS_SSH_USER=SrSainz`
- `NAS_SSH_PRIVATE_KEY=<clave privada dedicada para Vercel>`
- `NAS_BACKEND_HOST=127.0.0.1`
- `NAS_BACKEND_PORT=8780`

Con eso, `https://tiktok-xi-cyan.vercel.app` puede hablar con el backend del NAS sin exponer directamente el puerto interno del backend.

## Version NAS (recomendada para renders largos)

Tambien puedes mover el backend al NAS y dejar Vercel solo para la web, o usar la interfaz servida por el propio NAS.

### Rutas NAS

- App: `/home/SrSainz/apps/tiktok`
- UI local NAS: `http://IP_DEL_NAS:8780/studio`
- API health: `http://IP_DEL_NAS:8780/api/health`

### Variables

Copia `.env.example` a `.env` en el NAS y rellena:

- `CORS_ORIGINS`
- `YOUTUBE_API_KEY`
- `YOUTUBE_TREND_CATEGORY_IDS`
- `YTDLP_COOKIES_FILE`
- `OUTPUT_DIR`
- `WORK_DIR`

### Deploy

```bash
cd /home/SrSainz/apps/tiktok
chmod +x scripts/deploy_nas.sh
./scripts/deploy_nas.sh
```

### Actualizar desde GitHub

```bash
cd /home/SrSainz/apps/tiktok
./scripts/deploy_nas.sh
```
3. `Buscar virales`.
4. Selecciona URL y `Generar clips`.

## Parametros utiles

- `--duration 30` o `--duration 60`
- `--min-source-duration 95` para evitar videos demasiado cortos
- `--language es` para priorizar subtitulos en espanol
- `--per-channel-scan 20` numero de videos revisados por canal
- `--max-results 5` candidatos finales a ordenar por vistas

## Limites y notas

- Si el video no tiene subtitulos (normales o auto), el corto se genera sin subtitulos.
- La subida automatica depende de estado de login y cambios de UI de TikTok.
- Respeta derechos de autor y politicas de uso de YouTube/TikTok.
