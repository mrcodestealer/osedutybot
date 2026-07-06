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
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

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


# ================= Provider page (Playwright, authenticated) =================
def _render_provider(
    cookies: dict,
    provider_url: str,
    headless: bool,
    out_path=None,
    hold: bool = False,
):
    """Open the provider page with the logged-in session cookies.

    ``out_path``  — if given, save a full-page screenshot there (returns the path).
    ``hold``      — keep the browser window open until Enter is pressed (only useful
                    when ``headless`` is False, e.g. a local ``--headed`` demo).
    """
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
            saved = None
            if out_path is not None:
                page.screenshot(path=str(out_path), full_page=True)
                saved = str(out_path)
            if hold:
                try:
                    input("\n👀 Browser open on the Provider page. Press Enter to close…\n")
                except (EOFError, KeyboardInterrupt):
                    pass
            return saved
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


# ================= HTTP login =================
def _login_session(max_attempts: int) -> dict:
    """Log in over HTTP; return {ok, session, attempts, codes, captcha_image, message}.

    ``session`` is an authenticated ``requests.Session`` when ``ok`` is True.
    """
    ipbx = _ipbx_base()
    landing_url = f"{ipbx}/user!setSystemLocale.action"
    captcha_url = f"{ipbx}/securityCode!getSecurityCodeImg.action"
    login_url = f"{ipbx}/user!login.action"
    shots_dir = _shots_dir()
    timeout = _http_timeout()

    out = {
        "ok": False,
        "session": None,
        "attempts": 0,
        "codes": [],
        "captcha_image": None,
        "message": "",
    }
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (osedutybot changePrefix)"})
    pw_md5 = hashlib.md5(_password().encode("utf-8")).hexdigest()

    # 1) Establish a session (sets JSESSIONID).
    session.get(landing_url, timeout=timeout)

    last_reason = ""
    for attempt in range(1, max_attempts + 1):
        out["attempts"] = attempt

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
            try:
                session.get(landing_url, timeout=timeout)
            except Exception:
                pass
            continue
        ext = "png" if "png" in ctype else "jpg"
        cap_path = shots_dir / f"pldt_captcha_{attempt}.{ext}"
        cap_path.write_bytes(body)
        out["captcha_image"] = str(cap_path)

        # 3) OCR.
        code = _recognize_captcha(cap.content, mime=ctype or "image/jpeg")
        out["codes"].append(code)
        if not code:
            last_reason = "captcha OCR returned empty (check vision model)"
            continue

        # 4) Submit login. The page MD5-hashes the password client-side; we do the
        #    same here so ``user.pin`` matches.
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
                headers={"Referer": landing_url},
            )
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            last_reason = f"login POST failed: {exc!r}"
            print(f"[changePrefix] {last_reason}", flush=True)
            continue

        if not _is_login_page(resp.text):
            out["ok"] = True
            out["session"] = session
            out["message"] = f"Login OK on attempt {attempt} (code={code!r})."
            return out

        # Still a login page → failed. Retry only on a captcha error; any other
        # message (bad credentials, locked account, …) aborts so we don't burn
        # retries re-submitting the same password.
        tip = _extract_form_tip(resp.text)
        last_reason = tip or "login returned the login form (no message)"
        print(f"[changePrefix] attempt {attempt} failed: {last_reason!r}", flush=True)
        if tip and not _is_captcha_error(tip):
            out["message"] = (
                f"Login rejected: {last_reason} (attempt {attempt}). Aborting retries."
            )
            return out

    out["message"] = f"Failed after {max_attempts} attempt(s). Last: {last_reason}"
    return out


# ================= Main routine (login + screenshot) =================
def run_change_prefix(
    provider_id=None,
    headless=None,
    max_attempts=None,
    screenshot: bool = True,
    hold: bool = False,
) -> dict:
    provider_id = _provider_id(provider_id)
    headless = _headless(headless)
    max_attempts = _max_attempts(max_attempts)
    ipbx = _ipbx_base()
    provider_url = f"{ipbx}/providers!jumpEditProvider.action?id={provider_id}"
    shots_dir = _shots_dir()

    result = {
        "ok": False,
        "attempts": 0,
        "provider_id": provider_id,
        "message": "",
        "result_image": None,
        "captcha_image": None,
        "codes": [],
    }
    try:
        login = _login_session(max_attempts)
        result["attempts"] = login["attempts"]
        result["codes"] = login["codes"]
        result["captcha_image"] = login["captcha_image"]
        if not login["ok"]:
            result["message"] = login["message"]
            return result
        session = login["session"]

        if screenshot or hold:
            out = (shots_dir / f"pldt_provider_{provider_id}.png") if screenshot else None
            try:
                result["result_image"] = _render_provider(
                    session.cookies.get_dict(), provider_url, headless, out, hold
                )
            except Exception as exc:  # noqa: BLE001
                result["ok"] = False
                result["message"] = (
                    f"{login['message']} but opening the provider page failed: {exc!r}"
                )
                print(f"[changePrefix] provider page failed: {exc!r}", flush=True)
                return result
        result["ok"] = True
        extra = "" if screenshot else " (screenshot skipped)"
        result["message"] = f"{login['message']}{extra}"
        return result
    except Exception as exc:  # noqa: BLE001
        result["message"] = f"Error: {exc!r}"
        print(f"[changePrefix] error: {exc!r}", flush=True)
        return result


# ================= Prefix rotation (change value + before/after shots) =================
# Mapping from prefix.png (picture 3): left = CallerID Prefix set in the field,
# right = the active PLDT number announced to CS. Prefix 028-991-28NN (NN=80..99)
# maps to 9190599800..819; the announced number carries a leading 0.
def _build_prefix_map() -> dict:
    m = {}
    for nn in range(80, 100):
        m[f"02899128{nn:02d}"] = f"091905998{nn - 80:02d}"
    return m


PREFIX_TO_NUMBER = _build_prefix_map()
PREFIX_SEQUENCE = list(PREFIX_TO_NUMBER.keys())  # ordered 0289912880 → 0289912899
# Anchors verified against picture 3 (guards against a formula typo).
assert PREFIX_TO_NUMBER["0289912880"] == "09190599800"
assert PREFIX_TO_NUMBER["0289912894"] == "09190599814"
assert PREFIX_TO_NUMBER["0289912899"] == "09190599819"

_PREFIX_INPUT = "#callerPrefix"  # name=pbxdbSipProviderGateway.callerPrefix
_EDIT_FORM_ID = "fom"            # <form action="providers!editProvider.action">
_APPLY_BUTTON = "input[name='input2']"  # <input value="Apply" onclick="editProvider();">


def _apply_max_clicks() -> int:
    """How many times to click Apply before giving up (verifies + stops early)."""
    try:
        return max(1, int(os.getenv("PLDT_APPLY_CLICKS", "4")))
    except ValueError:
        return 4


def _click_apply_and_confirm(page) -> None:
    """Click the real Apply button and confirm the 'Submit success!' popup.

    Apply runs ``editProvider()`` → name-check AJAX → a ymPrompt confirm dialog whose
    OK calls ``form.submit()``. We click Apply, then click that OK; if the popup can't
    be found we submit ``#fom`` directly so the change still goes through. Then wait for
    the resulting page load.
    """
    try:
        page.click(_APPLY_BUTTON, timeout=_nav_timeout())
    except Exception as exc:  # noqa: BLE001
        print(f"[changePrefix] Apply button click failed: {exc!r}", flush=True)

    confirmed_modal = False
    for sel in (
        "#ymPrompt_btnV a",
        ".ymPrompt_btnV a",
        ".ymPrompt a:has-text('OK')",
        "a:has-text('OK')",
    ):
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=2000)
            loc.click(timeout=1500)
            confirmed_modal = True
            break
        except Exception:
            continue
    if not confirmed_modal:
        # Popup not found — guarantee the submit so the change still lands.
        try:
            page.evaluate(f"document.getElementById('{_EDIT_FORM_ID}').submit()")
        except Exception:
            pass
    try:
        page.wait_for_load_state("networkidle", timeout=_nav_timeout())
    except Exception:
        pass


def _next_prefix(current: str):
    """Return (new_prefix, message_number) for the +1 weekly rotation (2899 → 2880).

    ``message_number`` maps to the NEW prefix. Returns (None, None) if ``current`` is
    not one of the known 028-991-2880..2899 values (so the caller aborts instead of
    guessing).
    """
    cur = (current or "").strip()
    if cur not in PREFIX_TO_NUMBER:
        return None, None
    idx = PREFIX_SEQUENCE.index(cur)
    new_prefix = PREFIX_SEQUENCE[(idx + 1) % len(PREFIX_SEQUENCE)]
    return new_prefix, PREFIX_TO_NUMBER[new_prefix]


# ---- Idempotency state (prevents double-advancing the prefix) --------------
# The rotation is stateless per-run (it reads the live field and does +1), so a
# submit that COMMITS server-side but whose verify-reload fails would otherwise be
# retried and advance the prefix a SECOND time. We therefore record the rotation
# INTENT (target + ISO week) BEFORE submitting, and only ever advance once per ISO
# week: a same-week rerun reconciles against the live value instead of advancing.
def _state_path() -> Path:
    explicit = (os.getenv("PLDT_PREFIX_STATE_FILE") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(__file__).resolve().parent / ".pldt_prefix_state.json"


def _load_state() -> dict:
    try:
        return json.loads(_state_path().read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        path = _state_path()
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:  # noqa: BLE001
        print(f"[changePrefix] could not save rotation state: {exc!r}", flush=True)


def _iso_week_key() -> str:
    """ISO year-week in the rotation timezone (default Asia/Manila, UTC+8)."""
    tzname = (os.getenv("PLDT_PREFIX_TZ") or "Asia/Manila").strip()
    now = None
    if ZoneInfo is not None:
        try:
            now = datetime.now(ZoneInfo(tzname))
        except Exception:
            now = None
    if now is None:
        now = datetime.now()
    y, w, _ = now.isocalendar()
    return f"{y}-W{w:02d}"


def _rotate_in_browser(
    cookies: dict, provider_url: str, provider_id: str, shots_dir, dry_run: bool, headless: bool
) -> dict:
    """Screenshot before → decide target (idempotent per ISO week) → apply → screenshot after.

    Records the intent BEFORE submitting so a commit-then-verify-fail cannot double
    advance. ``already_applied`` is True when this ISO week's rotation was already done
    (the caller then skips the group announcement).
    """
    from playwright.sync_api import sync_playwright

    root = _ipbx_root() + "/"
    week = _iso_week_key()
    state = _load_state()
    res = {
        "ok": False,
        "old_prefix": None,
        "new_prefix": None,
        "message_number": None,
        "before_image": None,
        "after_image": None,
        "applied": False,
        "already_applied": False,
        "iso_week": week,
        "message": "",
    }
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        try:
            context.add_cookies(
                [{"name": n, "value": v, "url": root} for n, v in cookies.items() if v]
            )
            page = context.new_page()
            page.goto(provider_url, wait_until="domcontentloaded", timeout=_nav_timeout())
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

            # Screenshot BEFORE any change.
            before = shots_dir / "pldt_prefix_before.png"
            page.screenshot(path=str(before), full_page=True)
            res["before_image"] = str(before)

            # Read the current prefix straight from the field (source of truth).
            try:
                live = (page.input_value(_PREFIX_INPUT, timeout=_nav_timeout()) or "").strip()
            except Exception as exc:  # noqa: BLE001
                res["message"] = f"could not read the Prefix field ({_PREFIX_INPUT}): {exc!r}"
                return res

            # Decide the target for THIS run.
            same_week = (
                isinstance(state, dict)
                and state.get("iso_week") == week
                and str(state.get("provider_id")) == str(provider_id)
            )
            if same_week:
                target = state.get("target_prefix")
                from_prefix = state.get("from_prefix")
                number = PREFIX_TO_NUMBER.get(target or "")
                if live == target:
                    # This week's rotation already took effect (confirmed now, even if a
                    # previous run failed to verify). Do NOT advance again.
                    res.update(
                        old_prefix=from_prefix,
                        new_prefix=target,
                        message_number=number,
                        applied=True,
                        already_applied=True,
                        ok=True,
                        after_image=str(before),
                        message=f"already rotated this week ({from_prefix} → {target}); not advancing again",
                    )
                    # Make sure the confirmed state is persisted.
                    _save_state({
                        "iso_week": week, "provider_id": str(provider_id),
                        "from_prefix": from_prefix, "target_prefix": target, "applied": True,
                    })
                    return res
                elif live == from_prefix and number:
                    # Last submit did not commit — retry the SAME target (no advance).
                    new_prefix = target
                else:
                    res["message"] = (
                        f"this week's target was {target!r} (from {from_prefix!r}) but the live "
                        f"prefix is {live!r} — unexpected; aborting to avoid a wrong change"
                    )
                    return res
            else:
                # New week (or no state): advance once from the live value.
                from_prefix = live
                new_prefix, number = _next_prefix(live)
                if not new_prefix:
                    res["message"] = (
                        f"current prefix {live!r} is not in the known 028-991-2880..2899 "
                        "table — aborting to avoid setting a wrong value"
                    )
                    return res

            res["old_prefix"] = from_prefix
            res["new_prefix"] = new_prefix
            res["message_number"] = number

            if dry_run:
                res["ok"] = True
                res["message"] = (
                    f"[dry-run] would change {from_prefix} → {new_prefix}; CS number {number} "
                    "(no change applied)"
                )
                return res

            # Record the INTENT before submitting, so a commit-then-verify-fail is
            # reconciled (not double-advanced) on the next run.
            _save_state({
                "iso_week": week, "provider_id": str(provider_id),
                "from_prefix": from_prefix, "target_prefix": new_prefix, "applied": False,
            })

            # Apply by clicking the real Apply button — up to _apply_max_clicks() times
            # (default 4), because a single click sometimes doesn't persist. Between
            # clicks we reload and read the field back; as soon as it shows the new value
            # we stop (no point clicking more). Re-saving the same value is idempotent.
            max_clicks = _apply_max_clicks()
            confirmed = ""
            clicks_done = 0
            for i in range(1, max_clicks + 1):
                # Reload a clean edit page (this also verifies the previous click).
                try:
                    page.goto(provider_url, wait_until="domcontentloaded", timeout=_nav_timeout())
                    try:
                        page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    cur = (page.input_value(_PREFIX_INPUT, timeout=_nav_timeout()) or "").strip()
                except Exception:
                    cur = ""
                if cur == new_prefix:
                    confirmed = cur
                    print(
                        f"[changePrefix] Apply confirmed after {clicks_done} click(s) "
                        f"({from_prefix} → {new_prefix})",
                        flush=True,
                    )
                    break
                # Not saved yet → set the field and click Apply again.
                try:
                    page.fill(_PREFIX_INPUT, new_prefix)
                except Exception:
                    pass
                _click_apply_and_confirm(page)
                clicks_done = i
                print(f"[changePrefix] clicked Apply {i}/{max_clicks}", flush=True)

            # Final verification on a fresh reload (in case the last click just landed).
            if confirmed != new_prefix:
                try:
                    page.goto(provider_url, wait_until="domcontentloaded", timeout=_nav_timeout())
                    try:
                        page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass
                    confirmed = (page.input_value(_PREFIX_INPUT, timeout=_nav_timeout()) or "").strip()
                except Exception:
                    pass
            res["applied"] = confirmed == new_prefix

            # Screenshot AFTER the change.
            after = shots_dir / "pldt_prefix_after.png"
            page.screenshot(path=str(after), full_page=True)
            res["after_image"] = str(after)

            if res["applied"]:
                _save_state({
                    "iso_week": week, "provider_id": str(provider_id),
                    "from_prefix": from_prefix, "target_prefix": new_prefix, "applied": True,
                })
                res["ok"] = True
                res["message"] = (
                    f"changed {from_prefix} → {new_prefix} (Apply clicked {clicks_done}×); "
                    f"CS number {number}"
                )
            else:
                res["message"] = (
                    f"clicked Apply {clicks_done}× for {from_prefix} → {new_prefix} but the page "
                    f"read back {confirmed!r}; NOT confirmed. Intent is recorded, so a retry this "
                    "week will reconcile rather than double-advance."
                )
            return res
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


def rotate_prefix(provider_id=None, headless=None, max_attempts=None, dry_run: bool = False) -> dict:
    """Log in, then change the CallerID Prefix to the next value (with before/after shots).

    Idempotent per ISO week: at most one advance per week; a same-week rerun reconciles
    against the live value instead of advancing again. Returns {ok, dry_run, provider_id,
    attempts, old_prefix, new_prefix, message_number, before_image, after_image, applied,
    already_applied, iso_week, message, codes}. Does NOT post to Lark — main.py does.
    """
    provider_id = _provider_id(provider_id)
    headless = _headless(headless)
    max_attempts = _max_attempts(max_attempts)
    ipbx = _ipbx_base()
    provider_url = f"{ipbx}/providers!jumpEditProvider.action?id={provider_id}"
    shots_dir = _shots_dir()

    result = {
        "ok": False,
        "dry_run": dry_run,
        "provider_id": provider_id,
        "attempts": 0,
        "old_prefix": None,
        "new_prefix": None,
        "message_number": None,
        "before_image": None,
        "after_image": None,
        "applied": False,
        "already_applied": False,
        "iso_week": None,
        "message": "",
        "codes": [],
    }
    try:
        login = _login_session(max_attempts)
        result["attempts"] = login["attempts"]
        result["codes"] = login["codes"]
        if not login["ok"]:
            result["message"] = login["message"]
            return result
        rr = _rotate_in_browser(
            login["session"].cookies.get_dict(),
            provider_url,
            provider_id,
            shots_dir,
            dry_run,
            headless,
        )
        for k in (
            "old_prefix",
            "new_prefix",
            "message_number",
            "before_image",
            "after_image",
            "applied",
            "already_applied",
            "iso_week",
        ):
            result[k] = rr.get(k)
        result["ok"] = rr["ok"]
        result["message"] = rr["message"]
        return result
    except Exception as exc:  # noqa: BLE001
        result["message"] = f"Error: {exc!r}"
        print(f"[changePrefix] rotate error: {exc!r}", flush=True)
        return result


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if "--help" in args or "-h" in args:
        print(
            "Usage: python changePrefix.py [provider_id] [flags]\n"
            "  (default)         log in and screenshot the provider page\n"
            "  --headed          show a visible browser (default: headless)\n"
            "  --no-screenshot   don't save the provider-page PNG\n"
            "  --no-hold         with --headed, don't wait for Enter (close immediately)\n"
            "  --rotate          change the prefix to the next value (+1, wraps 2899→2880)\n"
            "  --dry-run         with --rotate: compute the new value but DON'T Apply\n"
            "\nExamples:\n"
            "  python changePrefix.py --headed --no-screenshot   # watch login, no file\n"
            "  python changePrefix.py --rotate --dry-run         # preview next prefix, no change\n"
            "  python changePrefix.py --rotate                   # ⚠️ really changes the prefix\n"
        )
        sys.exit(0)

    pid = next((a for a in args if a.isdigit()), None)
    headed = "--headed" in args or "--head" in args

    if "--rotate" in args:
        dry = "--dry-run" in args or "--dry" in args
        res = rotate_prefix(provider_id=pid, headless=not headed, dry_run=dry)
    else:
        no_shot = "--no-screenshot" in args or "--no-shot" in args
        # When headed, keep the window open so you can look at the page (login is HTTP,
        # so the browser is otherwise the only thing that would flash by).
        hold = headed and "--no-hold" not in args
        res = run_change_prefix(
            provider_id=pid, headless=not headed, screenshot=not no_shot, hold=hold
        )
    print("\n===== RESULT =====")
    for k, v in res.items():
        print(f"{k}: {v}")
