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
from urllib.parse import urlparse


JSON_LOG_MODE = False


def log(msg: str) -> None:
    target = sys.stderr if JSON_LOG_MODE else sys.stdout
    print(f"[tiktok-upload] {msg}", file=target)


PRIVACY_LABELS = {
    "SELF_ONLY": "Solo tú",
    "MUTUAL_FOLLOW_FRIENDS": "Amigos",
    "PUBLIC_TO_EVERYONE": "Todo el mundo",
}

PUBLISH_SUCCESS_TEXT_MARKERS = [
    "se ha publicado",
    "publicado",
    "published",
    "your video has been uploaded",
    "your post is being uploaded",
    "tu video se esta subiendo",
    "tu vídeo se está subiendo",
    "upload another",
    "subir otro",
]


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


def _detect_publish_success_from_text(raw_text: str) -> bool:
    normalized = _normalize_text(raw_text or "")
    return any(marker in normalized for marker in PUBLISH_SUCCESS_TEXT_MARKERS)


def _is_effectively_disabled(*values: str | None) -> bool:
    disabled_markers = {"true", "disabled", "1", "yes"}
    for value in values:
        text = _normalize_text(str(value or ""))
        if not text:
            continue
        if text in disabled_markers:
            return True
        if "disabled" in text:
            return True
    return False


def _normalize_caption_for_editor(caption: str) -> str:
    return (caption or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _caption_samples(caption_value: str) -> list[str]:
    lines = [line.strip() for line in _normalize_caption_for_editor(caption_value).splitlines() if line.strip()]
    samples: list[str] = []
    for line in lines[:3]:
        normalized = _normalize_text(line)
        if normalized:
            samples.append(normalized[:50])
    if samples:
        return samples
    fallback = _normalize_text(caption_value[:50])
    return [fallback] if fallback else []


def _text_contains_caption_sample(raw_text: str, caption_value: str) -> bool:
    samples = _caption_samples(caption_value)
    if not samples:
        return False
    haystack = _normalize_text(raw_text or "")
    return all(sample in haystack for sample in samples)


def _extract_playwright_caption_text(page, selector: str) -> str:
    try:
        return (
            page.evaluate(
                """
                (selector) => {
                  const el = document.querySelector(selector);
                  if (!el) return '';
                  return el.innerText || el.textContent || '';
                }
                """,
                selector,
            )
            or ""
        )
    except Exception:
        return ""


def _extract_selenium_caption_text(driver, element) -> str:
    try:
        return str(
            driver.execute_script(
                "return arguments[0].innerText || arguments[0].textContent || '';",
                element,
            )
            or ""
        )
    except Exception:
        try:
            return element.text or ""
        except Exception:
            return ""


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


def _set_playwright_caption(page, raw_caption: str) -> bool:
    caption_value = _normalize_caption_for_editor(raw_caption)
    if not caption_value:
        return True
    selectors = [
        ".public-DraftEditor-content[role='combobox']",
        "[role='combobox'].public-DraftEditor-content",
        "[data-e2e='video-caption-editor'] [contenteditable='true']",
        "div[contenteditable='true']",
        "div[contenteditable='plaintext-only']",
    ]
    for selector in selectors:
        try:
            caption_box = page.locator(selector).first
            if caption_box.count() == 0:
                continue
            caption_box.click(timeout=15000)
            try:
                caption_box.press("Control+A", timeout=3000)
                caption_box.press("Backspace", timeout=3000)
            except Exception:
                pass
            try:
                page.evaluate(
                    """
                    ([selector, value]) => {
                      const el = document.querySelector(selector);
                      if (!el) return false;
                      el.focus();
                      const range = document.createRange();
                      range.selectNodeContents(el);
                      const selection = window.getSelection();
                      selection.removeAllRanges();
                      selection.addRange(range);
                      try { document.execCommand('delete', false); } catch (e) {}
                      el.innerHTML = '';
                      el.textContent = value;
                      el.dispatchEvent(new InputEvent('input', {bubbles: true, data: value, inputType: 'insertText'}));
                      el.dispatchEvent(new Event('change', {bubbles: true}));
                      return true;
                    }
                    """,
                    [selector, caption_value],
                )
            except Exception:
                pass
            editor_text = _extract_playwright_caption_text(page, selector)
            if _text_contains_caption_sample(editor_text, caption_value):
                return True
            try:
                caption_box.click(timeout=5000)
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.keyboard.insert_text(caption_value)
                page.wait_for_timeout(500)
            except Exception:
                pass
            editor_text = _extract_playwright_caption_text(page, selector)
            if _text_contains_caption_sample(editor_text, caption_value):
                return True
        except Exception:
            continue
    return False


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


def _cdp_debugger_address(connect_cdp: str) -> str:
    parsed = urlparse(connect_cdp)
    if parsed.scheme and parsed.hostname and parsed.port:
        return f"{parsed.hostname}:{parsed.port}"
    return connect_cdp.replace("http://", "").replace("https://", "").strip().rstrip("/")


def _upload_via_selenium_cdp(
    *,
    connect_cdp: str,
    video_path: Path,
    caption: str,
    privacy_level: str,
    auto_post: bool,
    manual_wait: int,
) -> dict[str, str]:
    import time

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as exc:
        raise RuntimeError(f"SELENIUM_NOT_AVAILABLE: {exc}") from exc

    debugger_address = _cdp_debugger_address(connect_cdp)
    options = Options()
    options.debugger_address = debugger_address
    driver = webdriver.Chrome(options=options)

    def _find_publish_button():
        selectors = [
            "//button[contains(., 'Publicar') or contains(., 'Post')]",
            "//*[@role='button' and (contains(., 'Publicar') or contains(., 'Post'))]",
        ]
        for xpath in selectors:
            for element in driver.find_elements(By.XPATH, xpath):
                try:
                    if not element.is_displayed():
                        continue
                except Exception:
                    continue
                return element
        return None

    def _publish_button_is_ready():
        button = _find_publish_button()
        if button is None:
            return False
        try:
            disabled = _is_effectively_disabled(
                button.get_attribute("disabled"),
                button.get_attribute("aria-disabled"),
                button.get_attribute("class"),
                button.get_dom_attribute("disabled"),
            )
            return not disabled
        except Exception:
            return True

    def _click_publish_button(button) -> bool:
        strategies = []
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", button)
        except Exception:
            pass
        strategies.append(lambda: button.click())
        strategies.append(lambda: ActionChains(driver).move_to_element(button).pause(0.1).click(button).perform())
        strategies.append(lambda: driver.execute_script("arguments[0].click();", button))
        strategies.append(
            lambda: driver.execute_script(
                """
                const el = arguments[0];
                ['pointerdown','mousedown','pointerup','mouseup','click'].forEach((name) => {
                  el.dispatchEvent(new MouseEvent(name, {bubbles: true, cancelable: true, view: window}));
                });
                """,
                button,
            )
        )
        for strategy in strategies:
            try:
                strategy()
                return True
            except Exception:
                continue
        return False

    def _find_caption_box():
        selectors = [
            ".public-DraftEditor-content[role='combobox']",
            "[role='combobox'].public-DraftEditor-content",
            "[data-e2e='video-caption-editor'] [contenteditable='true']",
            "div[contenteditable='true']",
            "div[contenteditable='plaintext-only']",
        ]
        for selector in selectors:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                try:
                    if not element.is_displayed():
                        continue
                except Exception:
                    continue
                return element
        return None

    def _set_caption_safely(raw_caption: str) -> bool:
        caption_value = _normalize_caption_for_editor(raw_caption)
        if not caption_value:
            return True
        strategies = [
            lambda el: (
                el.click(),
                ActionChains(driver)
                .move_to_element(el)
                .click(el)
                .key_down(Keys.CONTROL)
                .send_keys("a")
                .key_up(Keys.CONTROL)
                .send_keys(Keys.BACKSPACE)
                .send_keys(caption_value)
                .perform()
            ),
            lambda el: (
                el.click(),
                el.send_keys(Keys.CONTROL, "a"),
                el.send_keys(Keys.BACKSPACE),
                el.send_keys(caption_value),
            ),
            lambda el: (
                driver.execute_script(
                    """
                    const el = arguments[0];
                    const value = arguments[1];
                    el.focus();
                    const range = document.createRange();
                    range.selectNodeContents(el);
                    const selection = window.getSelection();
                    selection.removeAllRanges();
                    selection.addRange(range);
                    try { document.execCommand('delete', false); } catch (e) {}
                    el.innerHTML = '';
                    el.textContent = value;
                    el.dispatchEvent(new InputEvent('input', {bubbles: true, data: value, inputType: 'insertText'}));
                    el.dispatchEvent(new Event('change', {bubbles: true}));
                    """,
                    el,
                    caption_value,
                )
            ),
        ]

        deadline = time.time() + 20
        while time.time() < deadline:
            box = _find_caption_box()
            if box is None:
                time.sleep(0.6)
                continue
            for strategy in strategies:
                try:
                    strategy(box)
                except Exception:
                    continue
                time.sleep(0.6)
                editor_text = _extract_selenium_caption_text(driver, _find_caption_box() or box)
                if _text_contains_caption_sample(editor_text, caption_value):
                    return True
            time.sleep(0.8)
        return False

    try:
        log(f"Conectado a navegador existente por Selenium/CDP: {debugger_address}")
        upload_url = "https://www.tiktok.com/tiktokstudio/upload?lang=es"

        try:
            current_handle = driver.current_window_handle
            for handle in list(driver.window_handles):
                driver.switch_to.window(handle)
                current_url = driver.current_url or ""
                if handle == current_handle:
                    continue
                if "tiktok.com/tiktokstudio/upload" in current_url:
                    try:
                        driver.close()
                    except Exception:
                        pass
            try:
                driver.switch_to.window(current_handle)
            except Exception:
                pass
        except Exception:
            pass

        driver.switch_to.new_window("tab")
        driver.get(upload_url)

        wait = WebDriverWait(driver, 60)

        def _find_file_input():
            selectors = [
                "input[type='file']",
                "input[accept*='video']",
                "input[data-e2e*='upload']",
            ]
            for selector in selectors:
                found = driver.find_elements(By.CSS_SELECTOR, selector)
                if found:
                    return found[0], selector
            return None, ""

        file_input, selector = _find_file_input()
        if not file_input:
            buttons = [
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ', 'abcdefghijklmnopqrstuvwxyzáéíóú'),'seleccionar vídeo')]",
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ', 'abcdefghijklmnopqrstuvwxyzáéíóú'),'seleccionar video')]",
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ', 'abcdefghijklmnopqrstuvwxyzáéíóú'),'sustituir')]",
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'upload')]",
                "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ', 'abcdefghijklmnopqrstuvwxyzáéíóú'),'cargar')]",
            ]
            for xpath in buttons:
                elems = driver.find_elements(By.XPATH, xpath)
                if elems:
                    try:
                        elems[0].click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", elems[0])
                    break
            wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "input[type='file'], input[accept*='video'], input[data-e2e*='upload']")) > 0)
            file_input, selector = _find_file_input()
        if not file_input:
            raise RuntimeError("BROWSER_FILE_INPUT_NOT_FOUND")

        file_input.send_keys(str(video_path))
        log(f"Video cargado (selenium:{selector or 'input'}): {video_path.name}")

        wait.until(lambda d: "upload" in (d.current_url or "") or len(d.find_elements(By.CSS_SELECTOR, "[data-e2e='video_visibility_container']")) > 0)
        wait.until(lambda d: _find_caption_box() is not None)
        time.sleep(2.0)

        if caption:
            caption_value = caption[:2200]
            if _set_caption_safely(caption_value):
                log(f"Caption aplicado ({len(caption_value)} chars).")
            else:
                raise RuntimeError("CAPTION_APPLY_FAILED")

        if privacy_level:
            try:
                target = _normalize_text(PRIVACY_LABELS.get(privacy_level, privacy_level))
                combo = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='video_visibility_container'] button[role='combobox']"))
                )
                try:
                    combo.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", combo)
                wait.until(
                    lambda d: len(
                        d.find_elements(By.CSS_SELECTOR, "div.Select__item, [role='option'], div[aria-selected]")
                    ) > 0
                )
                matched = False
                for element in driver.find_elements(By.CSS_SELECTOR, "div.Select__item, [role='option'], div[aria-selected]"):
                    text = _normalize_text(element.text)
                    if target and target in text:
                        try:
                            element.click()
                        except Exception:
                            driver.execute_script("arguments[0].click();", element)
                        matched = True
                        break
                if matched:
                    log(f"Privacidad ajustada a: {PRIVACY_LABELS.get(privacy_level, privacy_level)}")
                else:
                    log("No se encontro la opcion de privacidad deseada; se mantiene el valor actual.")
            except Exception:
                log("No se pudo ajustar la privacidad automaticamente; se mantiene el valor actual.")

        if auto_post:
            wait.until(lambda d: _find_publish_button() is not None)
            wait.until(lambda d: _publish_button_is_ready())
            publish_status = ""
            clicked = False
            for attempt in range(2):
                post_btn = _find_publish_button()
                if post_btn is None:
                    break
                clicked = _click_publish_button(post_btn)
                if not clicked:
                    continue
                log(f"Intentando publicar automaticamente (intento {attempt + 1})...")
                try:
                    def _published(driver_obj):
                        nonlocal publish_status
                        url = (driver_obj.current_url or "").lower()
                        if "upload" not in url:
                            publish_status = "publish_navigation_detected"
                            return True
                        try:
                            body = driver_obj.find_element(By.TAG_NAME, "body").text
                        except Exception:
                            body = ""
                        if _detect_publish_success_from_text(body):
                            publish_status = "publish_confirmation_detected"
                            return True
                        button = _find_publish_button()
                        if button is not None:
                            try:
                                button_text = _normalize_text(button.text)
                            except Exception:
                                button_text = ""
                            if "publicando" in button_text or "posting" in button_text or "subiendo" in button_text:
                                publish_status = "publish_in_progress"
                        return False

                    WebDriverWait(driver, 90).until(_published)
                except Exception:
                    pass
                if publish_status in {"publish_navigation_detected", "publish_confirmation_detected"}:
                    break
                if attempt == 0:
                    try:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    except Exception:
                        pass
            if publish_status in {"publish_navigation_detected", "publish_confirmation_detected"}:
                status = publish_status
            elif clicked:
                status = "post_clicked_unconfirmed"
            else:
                status = "publish_click_failed"
        else:
            log(f"Listo para revisar/publicar manualmente. Esperando {manual_wait}s...")
            import time
            time.sleep(max(1, manual_wait))
            status = "manual_review"

        return {"ok": "true", "status": status}
    finally:
        try:
            driver.quit()
        except Exception:
            pass


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
    if connect_cdp:
        return _upload_via_selenium_cdp(
            connect_cdp=connect_cdp,
            video_path=video_path,
            caption=caption,
            privacy_level=privacy_level,
            auto_post=auto_post,
            manual_wait=manual_wait,
        )
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
            caption_value = caption[:2200]
            if _set_playwright_caption(page, caption_value):
                log(f"Caption aplicado ({len(caption_value)} chars).")
            else:
                raise RuntimeError("CAPTION_APPLY_FAILED")

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
            publish_status = ""
            try:
                def _published() -> bool:
                    nonlocal publish_status
                    url = (page.url or "").lower()
                    if "upload" not in url:
                        publish_status = "publish_navigation_detected"
                        return True
                    try:
                        body_text = page.locator("body").inner_text(timeout=2000)
                    except Exception:
                        body_text = ""
                    if _detect_publish_success_from_text(body_text):
                        publish_status = "publish_confirmation_detected"
                        return True
                    return False

                page.wait_for_function("() => true", timeout=100)  # keep playwright event loop warm
                deadline_ms = 75000
                interval_ms = 1000
                elapsed = 0
                while elapsed < deadline_ms:
                    if _published():
                        break
                    page.wait_for_timeout(interval_ms)
                    elapsed += interval_ms
                status = publish_status or "post_clicked_unconfirmed"
            except Exception:
                page.wait_for_timeout(3000)
                status = "post_clicked_unconfirmed"
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
