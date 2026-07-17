"""
Index osedutybot source and inject relevant snippets into LLM chat (RAG).

Enable in .env:
  BOT_CODE_ASSIST=1
  BOT_CODE_ROOT=/root/osedutybot
  BOT_CODE_RAG=1
  BOT_CODE_EMBED_MODEL=nomic-embed-text

Pull embed model once: ``ollama pull nomic-embed-text``
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

_INDEX_LOCK = threading.Lock()
_INDEX: Optional[dict[str, Any]] = None
_INDEX_BUILDING = False

_CODE_Q_RE = re.compile(
    r"(?i)(?:"
    r"code|source|repo|git|file|function|class|module|script|implement|"
    r"\.py\b|\.env|chatagent|commandagent|main\.py|"
    r"how\s+does|what\s+does|where\s+is|which\s+file|"
    r"代码|源码|文件|函数|模块|实现|哪个文件|怎么实现"
    r")"
)

_SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "browser_data",
    "deep_person_chatbot_pt",
    "commandagent_pt",
    "command_intent_pt",
    "chatagent_pt",
    "chatagent_llm_pt",
    ".code_rag_cache",
    "disabled-amx",
    "testing",
    "robot_quick_start",
}

_SKIP_FILE_NAMES = {
    ".env",
    "cookies.json",
    "user_token.json",
    "webmachine_data.json",
    "allduty.json",
}

_SKIP_SUFFIXES = (
    ".pyc",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".log",
    ".bak",
    ".rpm",
    ".tar.xz",
    ".sh",
)

_INCLUDE_SUFFIXES = (".py", ".md", ".example", ".txt", ".json", ".yaml", ".yml", ".conf")


def is_enabled() -> bool:
    return (os.getenv("BOT_CODE_ASSIST") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def code_root() -> Path:
    raw = (os.getenv("BOT_CODE_ROOT") or "").strip()
    if raw:
        return Path(raw).resolve()
    return Path(__file__).resolve().parent


def _rag_enabled() -> bool:
    return (os.getenv("BOT_CODE_RAG") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _embed_model() -> str:
    return (os.getenv("BOT_CODE_EMBED_MODEL") or "nomic-embed-text").strip()


# When the embed model isn't pulled on the server, Ollama's /api/embeddings
# returns HTTP 404 for EVERY chunk. Without this guard a single index build
# logged ~1 failure per chunk (thousands of lines) and re-ran on every manifest
# change — 12k+ journal lines/day. Once we confirm the model is unavailable we
# stop calling the endpoint for the rest of the process, log ONE actionable
# line, and fall back to the grep search that already backs RAG.
_EMBED_STATE_LOCK = threading.Lock()
_EMBED_UNAVAILABLE = False


def _embed_unavailable() -> bool:
    with _EMBED_STATE_LOCK:
        return _EMBED_UNAVAILABLE


def _mark_embed_unavailable(reason: str) -> None:
    global _EMBED_UNAVAILABLE
    with _EMBED_STATE_LOCK:
        if _EMBED_UNAVAILABLE:
            return
        _EMBED_UNAVAILABLE = True
    print(
        f"[codeassist] embed model {_embed_model()!r} unavailable ({reason}) — "
        f"disabling RAG embeddings for this run, using grep fallback. "
        f"Fix: run `ollama pull {_embed_model()}` on the server (then restart), "
        f"or set BOT_CODE_RAG=0 to silence.",
        flush=True,
    )


def _top_k() -> int:
    try:
        return max(1, min(20, int(os.getenv("BOT_CODE_TOP_K", "8"))))
    except ValueError:
        return 8


def _max_context_chars() -> int:
    try:
        return max(2000, int(os.getenv("BOT_CODE_MAX_CONTEXT_CHARS", "14000")))
    except ValueError:
        return 14000


def _ollama_root() -> str:
    base = (
        os.getenv("BOT_CHAT_API_BASE") or "http://127.0.0.1:11434/v1"
    ).strip().rstrip("/")
    if base.endswith("/v1"):
        base = base[:-3]
    return base.rstrip("/")


def _cache_dir() -> Path:
    explicit = (os.getenv("BOT_CODE_CACHE_DIR") or "").strip()
    if explicit:
        return Path(explicit).resolve()
    return code_root() / ".code_rag_cache"


def _cache_path() -> Path:
    return _cache_dir() / "index.json"


def _should_index_file(path: Path, root: Path) -> bool:
    rel = path.relative_to(root)
    if path.name in _SKIP_FILE_NAMES:
        return False
    if path.name.startswith(".") and path.suffix not in (".example",):
        return False
    for part in rel.parts:
        if part in _SKIP_DIR_NAMES:
            return False
    if path.suffix.lower() in _SKIP_SUFFIXES:
        return False
    if path.suffix.lower() in _INCLUDE_SUFFIXES:
        if path.suffix.lower() == ".json" and path.stat().st_size > 512_000:
            return False
        return True
    return path.name in (".env.example", "Modelfile")


def _file_manifest(root: Path) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIR_NAMES]
        for name in filenames:
            path = Path(dirpath) / name
            try:
                if not _should_index_file(path, root):
                    continue
                rel = str(path.relative_to(root)).replace("\\", "/")
                stat = path.stat()
                manifest[rel] = f"{stat.st_mtime_ns}:{stat.st_size}"
            except OSError:
                continue
    return manifest


def _chunk_file(path: Path, root: Path, *, lines_per_chunk: int = 80, overlap: int = 15) -> list[dict[str, Any]]:
    rel = str(path.relative_to(root)).replace("\\", "/")
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    lines = text.splitlines()
    if not lines:
        return []
    chunks: list[dict[str, Any]] = []
    step = max(1, lines_per_chunk - overlap)
    for start in range(0, len(lines), step):
        end = min(len(lines), start + lines_per_chunk)
        block = "\n".join(lines[start:end]).strip()
        if len(block) < 40:
            continue
        chunks.append(
            {
                "path": rel,
                "start_line": start + 1,
                "end_line": end,
                "text": block[:6000],
            }
        )
        if end >= len(lines):
            break
    return chunks


def _ollama_embed(text: str) -> Optional[list[float]]:
    if _embed_unavailable():
        return None
    model = _embed_model()
    payload = json.dumps({"model": model, "prompt": text[:8000]}).encode("utf-8")
    req = urllib.request.Request(
        f"{_ollama_root()}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        emb = body.get("embedding")
        if isinstance(emb, list) and emb:
            return [float(x) for x in emb]
        # HTTP 200 but no vector — the model isn't producing embeddings.
        _mark_embed_unavailable("empty embedding in 200 response")
    except urllib.error.HTTPError as exc:
        # 404 = model not pulled / endpoint missing; 400 = model can't embed.
        # Either way retrying every chunk is pointless — disable and grep.
        if exc.code in (404, 400):
            _mark_embed_unavailable(f"HTTP {exc.code} {exc.reason}")
        else:
            print(f"[codeassist] embed failed ({model}): HTTP {exc.code} {exc.reason}", flush=True)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        print(f"[codeassist] embed failed ({model}): {exc!r}", flush=True)
    return None


def _cosine(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na <= 0 or nb <= 0:
        return 0.0
    return dot / (na * nb)


def _grep_fallback(query: str, root: Path, limit: int) -> list[dict[str, Any]]:
    terms = [t for t in re.split(r"\W+", query.lower()) if len(t) >= 3][:6]
    if not terms:
        terms = ["def ", "class "]
    hits: list[tuple[int, dict[str, Any]]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIR_NAMES]
        for name in filenames:
            path = Path(dirpath) / name
            if not _should_index_file(path, root):
                continue
            if path.suffix.lower() != ".py":
                continue
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            except OSError:
                continue
            rel = str(path.relative_to(root)).replace("\\", "/")
            for i, line in enumerate(lines, start=1):
                low = line.lower()
                score = sum(1 for t in terms if t in low)
                if score <= 0:
                    continue
                start = max(1, i - 3)
                end = min(len(lines), i + 12)
                block = "\n".join(lines[start - 1 : end])
                hits.append(
                    (
                        score,
                        {
                            "path": rel,
                            "start_line": start,
                            "end_line": end,
                            "text": block[:4000],
                            "score": float(score),
                        },
                    )
                )
    hits.sort(key=lambda x: x[0], reverse=True)
    return [h[1] for h in hits[:limit]]


def _build_index(root: Path) -> dict[str, Any]:
    manifest = _file_manifest(root)
    chunks: list[dict[str, Any]] = []
    for rel in sorted(manifest):
        path = root / rel
        chunks.extend(_chunk_file(path, root))

    indexed: list[dict[str, Any]] = []
    use_rag = _rag_enabled() and not _embed_unavailable()
    if use_rag:
        print(f"[codeassist] embedding {len(chunks)} chunks …", flush=True)
        for i, ch in enumerate(chunks):
            emb = _ollama_embed(ch["text"])
            if emb is None:
                # Model went unavailable (404/etc.) — stop probing every chunk;
                # the rest would all fail. _ollama_embed already logged once.
                if _embed_unavailable():
                    break
                continue
            item = dict(ch)
            item["embedding"] = emb
            indexed.append(item)
            if (i + 1) % 50 == 0:
                print(f"[codeassist] embedded {i + 1}/{len(chunks)}", flush=True)
        if not indexed:
            print("[codeassist] RAG embed empty — using grep fallback at query time", flush=True)
    elif _rag_enabled():
        print("[codeassist] embed model unavailable — grep-only index", flush=True)

    return {
        "version": 1,
        "root": str(root),
        "embed_model": _embed_model() if use_rag else "",
        "manifest": manifest,
        "manifest_hash": hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "chunks": indexed,
        "chunk_count_raw": len(chunks),
    }


def _manifest_stale(index: dict[str, Any], root: Path) -> bool:
    current = _file_manifest(root)
    return index.get("manifest") != current or index.get("root") != str(root)


def _load_index_from_disk() -> Optional[dict[str, Any]]:
    path = _cache_path()
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("chunks") is not None:
            return data
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _save_index_to_disk(index: dict[str, Any]) -> None:
    cache = _cache_dir()
    cache.mkdir(parents=True, exist_ok=True)
    _cache_path().write_text(json.dumps(index), encoding="utf-8")


def get_index(*, force_rebuild: bool = False) -> Optional[dict[str, Any]]:
    global _INDEX
    if not is_enabled():
        return None
    with _INDEX_LOCK:
        if _INDEX is not None and not force_rebuild:
            root = code_root()
            if not _manifest_stale(_INDEX, root):
                return _INDEX
        root = code_root()
        if not root.is_dir():
            print(f"[codeassist] BOT_CODE_ROOT not found: {root}", flush=True)
            return None
        cached = None if force_rebuild else _load_index_from_disk()
        if cached and not _manifest_stale(cached, root):
            if cached.get("embed_model", "") == (_embed_model() if _rag_enabled() else ""):
                _INDEX = cached
                print(
                    f"[codeassist] loaded cache: {len(cached.get('chunks') or [])} chunks",
                    flush=True,
                )
                return _INDEX
        print(f"[codeassist] building index under {root} …", flush=True)
        built = _build_index(root)
        _save_index_to_disk(built)
        _INDEX = built
        print(
            f"[codeassist] index ready: {len(built.get('chunks') or [])} embedded chunks "
            f"({built.get('chunk_count_raw', 0)} raw)",
            flush=True,
        )
        return _INDEX


def build_index_async() -> None:
    global _INDEX_BUILDING

    if not is_enabled():
        return

    def _worker() -> None:
        global _INDEX_BUILDING
        _INDEX_BUILDING = True
        try:
            get_index(force_rebuild=False)
        except Exception as exc:
            print(f"[codeassist] index build failed: {exc!r}", flush=True)
        finally:
            _INDEX_BUILDING = False

    if _INDEX_BUILDING:
        return
    threading.Thread(target=_worker, daemon=True, name="codeassist-index").start()


def looks_like_code_question(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    if _CODE_Q_RE.search(raw):
        return True
    if ".py" in raw.lower():
        return True
    return False


def _format_hits(hits: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    budget = _max_context_chars()
    used = 0
    for h in hits:
        header = f"### {h['path']}:{h.get('start_line', '?')}-{h.get('end_line', '?')}\n"
        body = (h.get("text") or "").strip()
        block = header + "```\n" + body + "\n```\n"
        if used + len(block) > budget:
            break
        parts.append(block)
        used += len(block)
    return "\n".join(parts)


def retrieve_hits(query: str) -> list[dict[str, Any]]:
    root = code_root()
    index = get_index()
    top_k = _top_k()
    q = (query or "").strip()
    if not q:
        return []

    if index and index.get("chunks") and _rag_enabled():
        q_emb = _ollama_embed(q)
        if q_emb:
            scored: list[tuple[float, dict[str, Any]]] = []
            for ch in index["chunks"]:
                emb = ch.get("embedding")
                if not isinstance(emb, list):
                    continue
                score = _cosine(q_emb, emb)
                if score <= 0.05:
                    continue
                hit = {k: v for k, v in ch.items() if k != "embedding"}
                hit["score"] = score
                scored.append((score, hit))
            scored.sort(key=lambda x: x[0], reverse=True)
            if scored:
                return [h for _, h in scored[:top_k]]

    return _grep_fallback(q, root, top_k)


def context_for_llm(query: str) -> str:
    if not is_enabled():
        return ""
    q = (query or "").strip()
    if not q:
        return ""
    always = (os.getenv("BOT_CODE_RAG_ALWAYS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if not always and not looks_like_code_question(q):
        return ""

    hits = retrieve_hits(q)
    if not hits:
        return ""
    root = code_root()
    formatted = _format_hits(hits)
    if not formatted:
        return ""
    return (
        f"\n\n--- osedutybot source ({root}) — use ONLY these excerpts to answer "
        f"code/architecture questions. Cite file paths. Do not invent files or APIs.\n"
        f"{formatted}"
    )


def startup_status() -> None:
    if not is_enabled():
        return
    root = code_root()
    print(
        f"[codeassist] enabled root={root} rag={_rag_enabled()} embed={_embed_model()}",
        flush=True,
    )
    build_index_async()

