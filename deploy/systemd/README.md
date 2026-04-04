# NAS Deployment

This service keeps the Clip Studio ES backend running on the NAS.

## Paths

- App: `/home/SrSainz/apps/tiktok`
- UI: `http://NAS_IP:8780/studio`
- API health: `http://NAS_IP:8780/api/health`

## Install

```bash
cd /home/SrSainz/apps/tiktok
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
mkdir -p ~/.config/systemd/user
cp deploy/systemd/tiktok-backend.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable tiktok-backend.service
systemctl --user restart tiktok-backend.service
```

## Update after `git push`

```bash
cd /home/SrSainz/apps/tiktok
git pull origin master
.venv/bin/pip install -r requirements.txt
systemctl --user restart tiktok-backend.service
```

## Verify

```bash
systemctl --user status tiktok-backend.service --no-pager
curl -s http://127.0.0.1:8780/api/health
curl -s http://127.0.0.1:8780/
```
