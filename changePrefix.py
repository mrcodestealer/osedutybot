#!/usr/bin/env python3
"""
Polylink UC (IPBX) — log in and screenshot the Provider edit page.

Command: ``/pldtprefix [id]``

The login page (``user!setSystemLocale.action``) has a broken ``bodyload()`` that
unconditionally redirects to itself, so a browser sitting on it just loops. We
therefore log in over **HTTP** (which is also faster and more reliable) and only
use Playwright at the end, with the authenticated session cookie injected, to
render + screenshot the Provider page.

Flow:
  1. ``GET providers!jumpEditProvider.action?id=<id>`` to obtain a JSESSIONID.
  2. ``GET securityCode!getSecurityCodeImg.action`` — the small 4-char captcha
     image (this IS the "picture 2" crop), bound to the session.
  3. Ask a vision LLM (Ollama, OpenAI-compatible) to read the 4 characters.
  4. ``POST user!login.action`` with ``user.username`` + ``user.pin`` (MD5 of the
     password, exactly like the page's ``md5()``) + ``user.securityCode``.
     If the response is the login form again (e.g. ``验证码不正确`` / captcha
     incorrect), fetch a fresh captcha and retry — up to
     ``PLDT_PREFIX_MAX_ATTEMPTS`` (default 10) times.
  5. On success, inject the session cookies into Chromium, open the Provider
     page and screenshot the whole page.

Public API:
  ``run_change_prefix(provider_id=3, headless=True, max_attempts=10) -> dict``
  returns ``{ok, attempts, provider_id, message, result_image, captcha_image, codes}``.

Captcha OCR uses ``qwen3.6:35b-a3b`` (vision-capable, confirmed on the local
Ollama). It is a *thinking* model, so on Ollama's ``/v1`` endpoint we send
``reasoning_effort="none"`` — otherwise it spends every token reasoning and
returns empty content (~90s). Override the model only via ``PLDT_CAPTCHA_MODEL``.
"""

import base64
import hashlib
import os
import re
import tempfile
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ================= Configuration =================
DEFAULT_BASE = "http://cp-ipbx-polylink-aws.neweb.me:8080"
DEFAULT_USER = "admin"
DEFAULT_PASS = "Ad@sn1407"


def _ipbx_root() -> str:
    """Server root, e.g. ``http://host:8080`` (no trailing slash)."""
    return (os.getenv("PLDT_IPBX_BASE") or DEFAULT_BASE).strip().rstrip("/")


def _ipbx_base() -> str:
    """The ``/ipbx`` app context base."""
    return f"{_ipbx_root()}/ipbx"


def _user() -> str:
    return (os.getenv("PLDT_IPBX_USER") or DEFAULT_USER).strip()


def _password() -> str:
    # Not stripped — a password may legitimately contain surrounding spaces.
    return os.getenv("PLDT_IPBX_PASS") or DEFAULT_PASS


def _company_id() -> str:
    return (os.getenv("PLDT_IPBX_COMPANY_ID") or "1").strip()


def _provider_id(explicit=None) -> str:
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    return (os.getenv("PLDT_PREFIX_PROVIDER_ID") or "3").strip()


def _max_attempts(explicit=None) -> int:
    if explicit is not None:
        try:
            return max(1, int(explicit))
        except (TypeError, ValueError):
            pass
    try:
        return max(1, int(os.getenv("PLDT_PREFIX_MAX_ATTEMPTS", "10")))
    except ValueError:
        return 10


def _headless(explicit=None) -> bool:
    if explicit is not None:
        return bool(explicit)
    return (os.getenv("PLDT_PREFIX_HEADLESS") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _http_timeout() -> int:
    try:
        return max(5, int(os.getenv("PLDT_HTTP_TIMEOUT", "25")))
    except ValueError:
        return 25


def _nav_timeout() -> int:
    try:
        return max(5000, int(os.getenv("PLDT_NAV_TIMEOUT", "30000")))
    except ValueError:
        return 30000


def _shots_dir() -> Path:
    explicit = (os.getenv("PLDT_PREFIX_SHOT_DIR") or "").strip()
    base = Path(explicit) if explicit else Path(tempfile.gettempdir()) / "pldt_prefix"
    base.mkdir(parents=True, exist_ok=True)
    return base


# ---- Vision LLM (captcha OCR) — reuses the bot's OpenAI-compatible Ollama endpoint.
def _api_base() -> str:
    return (
        os.getenv("PLDT_CAPTCHA_API_BASE")
        or os.getenv("BOT_CHAT_API_BASE")
        or "http://127.0.0.1:11434/v1"
    ).strip().rstrip("/")


def _api_key() -> str:
    return (
        os.getenv("PLDT_CAPTCHA_API_KEY")
        or os.getenv("BOT_CHAT_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or "ollama"
    ).strip()


def _captcha_model() -> str:
    # Pinned to qwen3.6:35b-a3b (vision-capable) per requirement — override only via
    # the explicit PLDT_CAPTCHA_MODEL env, never falling back to another chat model.
    return (os.getenv("PLDT_CAPTCHA_MODEL") or "qwen3.6:35b-a3b").strip()


def _is_ollama(base: str) -> bool:
    low = (base or "").lower()
    return "11434" in low or "ollama" in low


def _captcha_timeout() -> int:
    try:
        return max(5, int(os.getenv("PLDT_CAPTCHA_TIMEOUT", "60")))
    except ValueError:
        return 60


def _captcha_len() -> int:
    try:
        return max(1, int(os.getenv("PLDT_CAPTCHA_LEN", "4")))
    except ValueError:
        return 4


# ================= Captcha OCR =================
# Common words a chatty LLM might emit around the answer — dropped before we pick
# the code, so "The code is BCNG" doesn't get read as "code".
_OCR_FILLER = {
    "the", "code", "is", "this", "that", "captcha", "character", "characters",
    "image", "are", "read", "reads", "them", "answer", "here", "only", "and",
    "with", "shows", "show", "contains", "contain", "text", "digit", "digits",
    "letter", "letters", "following", "result", "says", "see", "reply", "value",
    "string", "four", "your", "output",
}


def _clean_code(raw: str, length: int) -> str:
    """Reduce an LLM reply to the captcha code: alnum only, ignoring filler words."""
    if not raw:
        return ""
    tokens = re.findall(r"[A-Za-z0-9]+", raw)
    pool = [t for t in tokens if t.lower() not in _OCR_FILLER] or tokens
    exact = [t for t in pool if len(t) == length]
    if exact:
        # A real captcha token often mixes letters and digits; prefer that, else
        # take the last exact-length token (the answer usually trails any preamble).
        with_digit = [t for t in exact if any(ch.isdigit() for ch in t)]
        return (with_digit or exact)[-1]
    joined = "".join(pool)
    return joined[:length]


def _recognize_captcha(img_bytes: bytes, mime: str = "image/jpeg") -> str:
    """Send the captcha image to a vision LLM; return the recognized code."""
    if not img_bytes:
        return ""
    length = _captcha_len()
    b64 = base64.standard_b64encode(img_bytes).decode("ascii")
    prompt = (
        f"This image is a website login CAPTCHA. It contains exactly {length} "
        "characters — letters (A-Z) and/or digits (0-9) — over noisy lines. "
        f"Read them and reply with ONLY those {length} characters. "
        "No spaces, no punctuation, no explanation."
    )
    base = _api_base()
    payload = {
        "model": _captcha_model(),
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{b64}"},
                    },
                ],
            }
        ],
        "max_tokens": 32,
        "temperature": 0,
    }
    if _is_ollama(base):
        # qwen3.6:35b-a3b is a *thinking* model. Ollama's /v1 endpoint ignores
        # "think" and only honors "reasoning_effort"; without disabling it the model
        # spends every token reasoning and returns EMPTY content (~90s). "none" makes
        # it answer the code directly (~4s). keep_alive avoids reloading 35B per retry.
        payload["reasoning_effort"] = "none"
        payload["think"] = False
        ka = (
            os.getenv("PLDT_CAPTCHA_KEEP_ALIVE")
            or os.getenv("BOT_CHAT_OLLAMA_KEEP_ALIVE")
            or "-1"
        ).strip()
        try:
            payload["keep_alive"] = int(ka)
        except ValueError:
            payload["keep_alive"] = ka
    url = f"{base}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_api_key()}",
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=_captcha_timeout())
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:  # noqa: BLE001 — network/LLM errors are expected & retried
        print(f"[changePrefix] captcha OCR request failed: {exc!r}", flush=True)
        return ""
    choices = body.get("choices") or []
    if not choices:
        print(f"[changePrefix] captcha OCR: no choices in response: {body}", flush=True)
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content") or ""
    if isinstance(content, list):  # some backends return content parts
        content = "".join(
            part.get("text", "") for part in content if isinstance(part, dict)
        )
    code = _clean_code(str(content), length)
    if not code:
        # Fallback: a terse thinking trace may hold the code when content is empty.
        reasoning = str(message.get("reasoning") or "")
        if 0 < len(reasoning) <= 80:
            code = _clean_code(reasoning, length)
    print(f"[changePrefix] captcha OCR raw={content!r} -> code={code!r}", flush=True)
    return code


# ================= Login response analysis =================
def _is_login_page(text: str) -> bool:
    """A login page still shows the form; the app pages do not."""
    return ("loginForm" in text) or ('id="username"' in text)


_FORM_TIP_RE = re.compile(r'class="form_tip"[^>]*>(.*?)</div>', re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")


def _extract_form_tip(text: str) -> str:
    m = _FORM_TIP_RE.search(text or "")
    if not m:
        return ""
    return _TAG_RE.sub("", m.group(1)).strip()


# The captcha error is the ONLY failure we should retry (a fresh captcha may work).
# Any other login-form message (bad password, locked account, …) means retrying is
# pointless, so we treat "not a captcha error" as fatal. Observed captcha messages:
# ``验证码不正确`` (zh) and ``The Security Code Error`` (en).
_CAPTCHA_ERROR_MARKERS = (
    "验证码", "验证", "security code", "securitycode", "captcha", "code error",
)


def _is_captcha_error(tip: str) -> bool:
    low = (tip or "").lower()
    return any(m.lower() in low for m in _CAPTCHA_ERROR_MARKERS)


# ================= Screenshot (Playwright, authenticated) =================
def _screenshot_provider(cookies: dict, provider_url: str, out_path: Path, headless: bool) -> str:
    """Open the provider page with the logged-in session cookies and screenshot it."""
    from playwright.sync_api import sync_playwright

    root = _ipbx_root() + "/"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        try:
            context.add_cookies(
                [
                    {"name": name, "value": value, "url": root}
                    for name, value in cookies.items()
                    if value
                ]
            )
            page = context.new_page()
            page.goto(provider_url, wait_until="domcontentloaded", timeout=_nav_timeout())
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            page.screenshot(path=str(out_path), full_page=True)
            return str(out_path)
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


# ================= Main routine =================
def run_change_prefix(provider_id=None, headless=None, max_attempts=None) -> dict:
    provider_id = _provider_id(provider_id)
    headless = _headless(headless)
    max_attempts = _max_attempts(max_attempts)
    ipbx = _ipbx_base()
    provider_url = f"{ipbx}/providers!jumpEditProvider.action?id={provider_id}"
    captcha_url = f"{ipbx}/securityCode!getSecurityCodeImg.action"
    login_url = f"{ipbx}/user!login.action"
    shots_dir = _shots_dir()
    timeout = _http_timeout()

    result = {
        "ok": False,
        "attempts": 0,
        "provider_id": provider_id,
        "message": "",
        "result_image": None,
        "captcha_image": None,
        "codes": [],
    }

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (osedutybot changePrefix)"})
    pw_md5 = hashlib.md5(_password().encode("utf-8")).hexdigest()

    try:
        # 1) Establish a session (also sets JSESSIONID).
        session.get(provider_url, timeout=timeout)

        last_reason = ""
        for attempt in range(1, max_attempts + 1):
            result["attempts"] = attempt

            # 2) Fresh captcha bound to this session.
            try:
                cap = session.get(captcha_url, timeout=timeout)
                cap.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                last_reason = f"captcha fetch failed: {exc!r}"
                print(f"[changePrefix] {last_reason}", flush=True)
                continue
            ctype = (cap.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip()
            body = cap.content or b""
            is_image = (
                ctype.lower().startswith("image/")
                or body[:3] == b"\xff\xd8\xff"          # JPEG
                or body[:8] == b"\x89PNG\r\n\x1a\n"      # PNG
                or body[:6] in (b"GIF87a", b"GIF89a")    # GIF
            )
            if not is_image:
                last_reason = (
                    f"captcha endpoint returned non-image ({ctype}, {len(body)}B) — "
                    "session may have expired"
                )
                print(f"[changePrefix] {last_reason}", flush=True)
                # Try to re-establish the session before the next attempt.
                try:
                    session.get(provider_url, timeout=timeout)
                except Exception:
                    pass
                continue
            ext = "png" if "png" in ctype else "jpg"
            cap_path = shots_dir / f"pldt_captcha_{attempt}.{ext}"
            cap_path.write_bytes(body)
            result["captcha_image"] = str(cap_path)

            # 3) OCR.
            code = _recognize_captcha(cap.content, mime=ctype or "image/jpeg")
            result["codes"].append(code)
            if not code:
                last_reason = "captcha OCR returned empty (check vision model)"
                continue

            # 4) Submit login. The page MD5-hashes the password client-side; we do
            #    the same here so ``user.pin`` matches.
            data = {
                "user.pbxdbCompany.id": _company_id(),
                "user.username": _user(),
                "user.pin": pw_md5,
                "user.securityCode": code,
                "submit": "Login",
            }
            try:
                resp = session.post(
                    login_url,
                    data=data,
                    timeout=timeout,
                    allow_redirects=True,
                    headers={"Referer": f"{ipbx}/user!setSystemLocale.action"},
                )
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                last_reason = f"login POST failed: {exc!r}"
                print(f"[changePrefix] {last_reason}", flush=True)
                continue

            if not _is_login_page(resp.text):
                # 5) Logged in — screenshot the provider page with these cookies.
                #    Only report success if the screenshot (the whole point) works.
                out = shots_dir / f"pldt_provider_{provider_id}.png"
                try:
                    result["result_image"] = _screenshot_provider(
                        session.cookies.get_dict(), provider_url, out, headless
                    )
                except Exception as exc:  # noqa: BLE001
                    result["ok"] = False
                    result["message"] = (
                        f"Login OK on attempt {attempt} (code={code!r}) but the "
                        f"screenshot failed: {exc!r}"
                    )
                    print(f"[changePrefix] screenshot failed: {exc!r}", flush=True)
                    return result
                result["ok"] = True
                result["message"] = f"Login OK on attempt {attempt} (code={code!r})."
                return result

            # Still a login page → failed. Retry only on a captcha error; any other
            # message (bad credentials, locked account, …) aborts so we don't burn
            # retries re-submitting the same password.
            tip = _extract_form_tip(resp.text)
            last_reason = tip or "login returned the login form (no message)"
            print(f"[changePrefix] attempt {attempt} failed: {last_reason!r}", flush=True)
            if tip and not _is_captcha_error(tip):
                result["message"] = (
                    f"Login rejected: {last_reason} (attempt {attempt}). "
                    "Aborting retries."
                )
                return result
            # else captcha wrong / unknown → next loop fetches a new captcha.

        result["message"] = f"Failed after {max_attempts} attempt(s). Last: {last_reason}"
        return result
    except Exception as exc:  # noqa: BLE001
        result["message"] = f"Error: {exc!r}"
        print(f"[changePrefix] error: {exc!r}", flush=True)
        return result


if __name__ == "__main__":
    import sys

    pid = next((a for a in sys.argv[1:] if a.isdigit()), None)
    res = run_change_prefix(provider_id=pid, headless="--headed" not in sys.argv)
    print("\n===== RESULT =====")
    for k, v in res.items():
        print(f"{k}: {v}")
