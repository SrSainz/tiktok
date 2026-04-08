#!/usr/bin/env python
"""Upload a local MP4 to TikTok via Playwright."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from pathlib import Path


JSON_LOG_MODE = False


def log(msg: str) -> None:
    target = sys.stderr if JSON_LOG_MODE else sys.stdout
    print(f"[tiktok-upload] {msg}", file=target)


PRIVACY_LABELS = {
    "SELF_ONLY": "Solo tú",
    "MUTUAL_FOLLOW_FRIENDS": "Amigos",
    "PUBLIC_TO_EVERYONE": "Todo el mundo",
}


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).strip().lower()


def _current_privacy_text(visibility) -> str:
    try:
        return _normalize_text(visibility.inner_text(timeout=3000))
    except Exception:
        return ""


def _iter_privacy_candidates(page):
    selectors = [
        "div.Select__item",
        "[role='option']",
        "div[aria-selected]",
    ]
    seen: set[tuple[str, str, int]] = set()
    for selector in selectors:
        locator = page.locator(selector)
        try:
            count = locator.count()
        except Exception:
            count = 0
        for idx in range(count):
            item = locator.nth(idx)
            try:
                text = _normalize_text(item.inner_text(timeout=1000))
            except Exception:
                continue
            key = (selector, text, idx)
            if not text or key in seen:
                continue
            seen.add(key)
            yield item, text


def _click_privacy_via_dom(page, target: str) -> bool:
    try:
        page.wait_for_function(
            "() => document.querySelectorAll(\"div.Select__item, [role='option'], div[aria-selected]\").length > 0",
            timeout=5000,
        )
    except Exception:
        return False

    return bool(
        page.evaluate(
            """
            (target) => {
              const normalize = (value) =>
                (value || "")
                  .normalize("NFKD")
                  .replace(/[\\u0300-\\u036f]/g, "")
                  .trim()
                  .toLowerCase();
              const selectors = ["div.Select__item", "[role='option']", "div[aria-selected]"];
              for (const selector of selectors) {
                const nodes = Array.from(document.querySelectorAll(selector));
                for (const node of nodes) {
                  if (normalize(node.innerText || node.textContent || "").includes(target)) {
                    node.click();
                    return true;
                  }
                }
              }
              return false;
            }
            """,
            target,
        )
    )


def _apply_privacy_with_keyboard(page, visibility, privacy_level: str) -> bool:
    target = _normalize_text(PRIVACY_LABELS.get(privacy_level, privacy_level))
    if _current_privacy_text(visibility) == target:
        return True

    for _attempt in range(3):
        visibility.click(timeout=10000)
        page.wait_for_timeout(500)
        matched = _click_privacy_via_dom(page, target)
        for item, text in _iter_privacy_candidates(page):
            if target in text:
                item.click(timeout=10000, force=True)
                page.wait_for_timeout(700)
                matched = True
                break
        if _current_privacy_text(visibility) == target:
            return True
        if matched:
            page.wait_for_timeout(300)
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        page.wait_for_timeout(250)
    return _current_privacy_text(visibility) == target


def _pick_existing_upload_page(context):
    candidates = []
    for page in context.pages:
        try:
            if "tiktokstudio/upload" not in page.url:
                continue
            body_text = page.locator("body").inner_text(timeout=2500)
            score = 0
            if "Publicar" in body_text or "Post" in body_text:
                score += 3
            if "Cargado" in body_text or "Loaded" in body_text:
                score += 2
            if "Guardar borrador" in body_text or "Draft" in body_text:
                score += 1
            candidates.append((score, page))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda row: row[0], reverse=True)
    return candidates[0][1]


def _find_first_button(page, names):
    for name in names:
        try:
            button = page.get_by_role("button", name=name).first
            if button.count() > 0:
                return button
        except Exception:
            continue
    return None


def _set_video_file(page, video_path: Path) -> str:
    selectors = ["input[type='file']", "input[accept*='video']", "input[data-e2e*='upload']"]
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.count() > 0:
                locator.set_input_files(str(video_path), timeout=15000)
                return f"input:{sel}"
            page.wait_for_selector(sel, timeout=3000, state="attached")
            page.locator(sel).first.set_input_files(str(video_path), timeout=15000)
            return f"input:{sel}"
        except Exception:
            continue

    upload_buttons = [
        "Sustituir",
        re.compile(r"Seleccionar v[ií]deo", re.I),
        re.compile(r"Seleccionar video", re.I),
        re.compile(r"Upload", re.I),
        re.compile(r"Cargar", re.I),
    ]
    button = _find_first_button(page, upload_buttons)
    if button is None:
        raise RuntimeError("BROWSER_FILE_INPUT_NOT_FOUND")

    try:
        with page.expect_file_chooser(timeout=10000) as chooser_info:
            button.click(timeout=10000, force=True)
        chooser_info.value.set_files(str(video_path))
        return "file_chooser_button"
    except Exception as exc:
        raise RuntimeError(f"BROWSER_FILE_INPUT_NOT_FOUND: {exc}") from exc


def pick_latest_video(output_dir: Path) -> Path:
    files = sorted(output_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise RuntimeError(f"No hay .mp4 en {output_dir}")
    return files[0]


def upload(
    video_path: Path,
    caption: str,
    privacy_level: str,
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

        page = _pick_existing_upload_page(context) if using_cdp else None
        if page is None:
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            log("Abriendo TikTok upload...")
            page.goto("https://www.tiktok.com/tiktokstudio/upload?lang=es", wait_until="domcontentloaded")
        else:
            log("Reutilizando pestaña existente de TikTok Studio.")
            page.bring_to_front()

        try:
            upload_method = _set_video_file(page, video_path)
        except RuntimeError as exc:
            log(f"No se detecto input de carga util. Puede ser bloqueo/login. Te dejo el navegador abierto para hacerlo manual. Motivo: {exc}")
            page.wait_for_timeout(manual_wait * 1000)
            return {"ok": "false", "status": str(exc)}

        log(f"Video cargado ({upload_method}): {video_path.name}")
        page.wait_for_timeout(1500)

        if caption:
            try:
                caption_box = page.locator("div[contenteditable='true']").first
                caption_box.click(timeout=15000)
                caption_box.fill("")
                page.keyboard.insert_text(caption[:2200])
            except Exception:
                log("No se pudo autocompletar caption; revisa manualmente.")

        if privacy_level:
            try:
                privacy_label = PRIVACY_LABELS.get(privacy_level, privacy_level)
                visibility = page.locator("[data-e2e='video_visibility_container'] button[role='combobox']").first
                if _apply_privacy_with_keyboard(page, visibility, privacy_level):
                    log(f"Privacidad ajustada a: {privacy_label}")
                else:
                    log("No se encontro la opcion de privacidad deseada; se mantiene el valor actual.")
            except Exception:
                log("No se pudo ajustar la privacidad automaticamente; se mantiene el valor actual.")

        if auto_post:
            post_btn = page.get_by_role("button", name=re.compile(r"(Publicar|Post)", re.I)).first
            post_btn.click(timeout=20000)
            log("Intentando publicar automaticamente...")
            try:
                page.wait_for_url(re.compile(r"^https://www\.tiktok\.com/(?!.*upload)"), timeout=45000)
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
    p.add_argument("--privacy-level", default="SELF_ONLY")
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
    global JSON_LOG_MODE
    args = build_parser().parse_args()
    JSON_LOG_MODE = bool(args.json)
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
            privacy_level=args.privacy_level,
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
        if args.json:
            print(json.dumps({"ok": "false", "status": "interrupted", "error": "Interrumpido por usuario."}, ensure_ascii=False))
        return 130
    except Exception as exc:
        log(f"ERROR: {exc}")
        if args.json:
            print(json.dumps({"ok": "false", "status": "error", "error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
