#!/usr/bin/env python
"""Upload a local MP4 to TikTok via Playwright."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path


def log(msg: str) -> None:
    print(f"[tiktok-upload] {msg}")


def pick_latest_video(output_dir: Path) -> Path:
    files = sorted(output_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise RuntimeError(f"No hay .mp4 en {output_dir}")
    return files[0]


def upload(
    video_path: Path,
    caption: str,
    profile_dir: Path,
    headless: bool,
    auto_post: bool,
    manual_wait: int,
    browser_channel: str,
    browser_executable: str,
    use_system_chrome_profile: bool,
    chrome_user_data_dir: str,
    chrome_profile_directory: str,
    connect_cdp: str,
) -> dict[str, str]:
    try:
        from playwright.sync_api import TimeoutError as PwTimeout
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright no esta instalado. Ejecuta: pip install playwright && playwright install chromium"
        ) from exc

    with sync_playwright() as p:
        browser = None
        using_cdp = bool(connect_cdp)
        if connect_cdp:
            browser = p.chromium.connect_over_cdp(connect_cdp)
            if browser.contexts:
                context = browser.contexts[0]
            else:
                context = browser.new_context(viewport={"width": 1400, "height": 900})
            log(f"Conectado a navegador existente por CDP: {connect_cdp}")
        else:
            launch_args = ["--start-maximized", "--disable-blink-features=AutomationControlled"]
            user_data_dir = str(profile_dir)
            if use_system_chrome_profile:
                user_data_dir = chrome_user_data_dir
                if chrome_profile_directory:
                    launch_args.append(f"--profile-directory={chrome_profile_directory}")
                log("Usando perfil real de Chrome. Cierra todas las ventanas de Chrome antes de ejecutar.")

            launch_kwargs = {
                "user_data_dir": user_data_dir,
                "headless": headless,
                "args": launch_args,
                "viewport": {"width": 1400, "height": 900},
            }
            if browser_channel in {"chrome", "msedge", "chromium"}:
                launch_kwargs["channel"] = browser_channel
            if browser_executable:
                launch_kwargs["executable_path"] = browser_executable

            context = p.chromium.launch_persistent_context(**launch_kwargs)

        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        log("Abriendo TikTok upload...")
        page.goto("https://www.tiktok.com/upload?lang=es", wait_until="domcontentloaded")

        file_selector = None
        selectors = ["input[type='file']", "input[accept*='video']", "input[data-e2e*='upload']"]
        for sel in selectors:
            try:
                page.wait_for_selector(sel, timeout=35000)
                file_selector = sel
                break
            except Exception:
                continue

        if not file_selector:
            log("No se detecto input de carga. Puede ser bloqueo/login. Te dejo el navegador abierto para hacerlo manual.")
            page.wait_for_timeout(manual_wait * 1000)
            return {"ok": "false", "status": "file_input_not_found"}

        page.set_input_files(file_selector, str(video_path))
        log(f"Video cargado: {video_path.name}")

        if caption:
            try:
                caption_box = page.locator("div[contenteditable='true']").first
                caption_box.click(timeout=15000)
                caption_box.fill("")
                page.keyboard.insert_text(caption[:2200])
            except Exception:
                log("No se pudo autocompletar caption; revisa manualmente.")

        if auto_post:
            post_btn = page.get_by_role("button", name=re.compile(r"(Publicar|Post)", re.I)).first
            post_btn.click(timeout=20000)
            log("Intentando publicar automaticamente...")
            try:
                page.wait_for_url(re.compile(r"^https://www\.tiktok\.com/(?!upload)"), timeout=45000)
                status = "publish_navigation_detected"
            except Exception:
                page.wait_for_timeout(15000)
                status = "post_clicked"
        else:
            log(f"Listo para revisar/publicar manualmente. Esperando {manual_wait}s...")
            page.wait_for_timeout(manual_wait * 1000)
            status = "manual_review"

        if not using_cdp:
            context.close()
        return {"ok": "true", "status": status}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Upload an already-rendered video to TikTok.")
    p.add_argument("--video", default="", help="Ruta a .mp4")
    p.add_argument("--latest-from", default="output", help="Carpeta para detectar ultimo .mp4")
    p.add_argument("--caption", default="")
    p.add_argument("--profile-dir", default=".tiktok_profile")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--auto-post", action="store_true")
    p.add_argument("--manual-wait", type=int, default=240)
    p.add_argument("--browser-channel", choices=["chrome", "msedge", "chromium", "brave"], default="chrome")
    p.add_argument("--browser-executable", default="")
    p.add_argument("--connect-cdp", default="")
    p.add_argument("--use-system-chrome-profile", action="store_true")
    p.add_argument(
        "--chrome-user-data-dir",
        default=os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "User Data"),
    )
    p.add_argument("--chrome-profile-directory", default="Default")
    p.add_argument(
        "--brave-user-data-dir",
        default=os.path.join(os.environ.get("LOCALAPPDATA", ""), "BraveSoftware", "Brave-Browser", "User Data"),
    )
    p.add_argument("--brave-profile-directory", default="Default")
    p.add_argument("--json", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    try:
        video = Path(args.video) if args.video else pick_latest_video(Path(args.latest_from))
        if not video.exists():
            raise RuntimeError(f"No existe el video: {video}")

        browser_executable = args.browser_executable
        use_system_profile = args.use_system_chrome_profile
        user_data_dir = args.chrome_user_data_dir
        profile_directory = args.chrome_profile_directory

        if args.browser_channel == "brave":
            if not browser_executable:
                browser_executable = os.path.join(
                    os.environ.get("PROGRAMFILES", r"C:\Program Files"),
                    "BraveSoftware",
                    "Brave-Browser",
                    "Application",
                    "brave.exe",
                )
            use_system_profile = True
            user_data_dir = args.brave_user_data_dir
            profile_directory = args.brave_profile_directory

        result = upload(
            video_path=video,
            caption=args.caption,
            profile_dir=Path(args.profile_dir),
            headless=args.headless,
            auto_post=args.auto_post,
            manual_wait=args.manual_wait,
            browser_channel=args.browser_channel,
            browser_executable=browser_executable,
            use_system_chrome_profile=use_system_profile,
            chrome_user_data_dir=user_data_dir,
            chrome_profile_directory=profile_directory,
            connect_cdp=args.connect_cdp,
        )
        if args.json:
            print(json.dumps(result, ensure_ascii=False))
        return 0
    except KeyboardInterrupt:
        log("Interrumpido por usuario.")
        return 130
    except Exception as exc:
        log(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
