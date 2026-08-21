#!/usr/bin/env python3
"""
``/isp <ip> [ip ...]`` — ISP / Organization, Country and ASN for one or more IPs.

The command renders a Lark card whose **IP Details** block is exactly three
bullets, with a provenance chip on each line::

    IP Details
    • ISP / Organization: Globe Telecom Inc.     [IPinfo +4]
    • Country: 🇵🇭 Philippines                    [IPinfo +4]
    • ASN: AS4775 / AS132199                     [IPinfo +4]

Values are merged across several key-free public IP-intel APIs, and across all
the IPs in one command — which is where ``AS4775 / AS132199`` comes from: three
Globe Telecom IPs that sit behind two different autonomous systems.

Providers (all verified working with no API key; ``IPINFO_TOKEN`` upgrades the
IPinfo response when set):

===================  ===================================================
Source               Fields used
===================  ===================================================
IPinfo               ``org`` ("AS4775 Globe Telecoms"), country code, city
ip-api               ``isp``, ``org``, ``as``, mobile/proxy/hosting flags
ipwho.is             ``connection.{asn,isp,org,domain}``, country, flag
ipapi.is             ``asn_num``, ``asn_org``, ``company_name``, vpn/tor
RIPEstat             announced prefix + authoritative ASN holder
iplocation.net       ``isp``, ``country_name``
===================  ===================================================

Merging is deliberately opinionated, because raw provider output does not read
like the card above — the same carrier comes back as "Globe Telecom", "Globe
Telecoms", "Globe Telecom Inc.", "Globe Telecom (GMCR,INC)" and
"GLOBE-TELECOM-AS". So names are grouped on a normalised key (punctuation and
legal suffixes dropped, trailing plurals folded) and each group is displayed
using its most *official-looking* variant — the one carrying a legal suffix, in
mixed case, without parenthetical noise. That is what turns those five strings
into a single "Globe Telecom Inc.".

Names that merely *contain* another name are folded too, so "Datacamp Limited"
and "CDN77 Datacamp Limited" read as one company — with a guard that keeps a
lone industry word like "Telecom" from pulling unrelated carriers together.
Diacritics are folded before the key is built, because otherwise "Orange Côte
d'Ivoire" and "ORANGE COTE D'IVOIRE" would key differently and the card would
name the same carrier twice.

The chip names the highest-priority provider that supplied the line plus how
many others corroborated it, so ``IPinfo +2`` means IPinfo and two more sources
agreed. Chips are card JSON v2 inline ``<text_tag>``, which only the v2
``markdown`` component interprets — so ``build_card`` emits ``markdown``
components rather than the v1 ``div``/``lark_md`` pair used by every other card
in this repo. That deviation is the one thing here without local precedent, so
``BOT_ISP_CARD_CHIPS=0`` switches back to ``div``/``lark_md`` with plain
``[IPinfo +2]`` chips if a tenant renders the tags literally.
"""

from __future__ import annotations

import ipaddress
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Optional

import requests

# Display priority. The first provider in this tuple that supplies a value both
# picks the wording shown for it and names the chip.
_PROVIDERS: tuple[tuple[str, str], ...] = (
    ("ipinfo", "IPinfo"),
    ("ipapi", "ip-api"),
    ("ipwhois", "ipwho.is"),
    ("ipapiis", "ipapi.is"),
    ("ripe", "RIPEstat"),
    ("iplocation", "iplocation.net"),
)
_PROVIDER_INDEX = {key: i for i, (key, _name) in enumerate(_PROVIDERS)}
_PROVIDER_NAME = {key: name for key, name in _PROVIDERS}

USAGE = (
    "❌ Usage: `/isp <ip> [ip ...]`\n"
    "Examples: `/isp 112.198.1.1`, `/isp 112.198.1.1 203.177.42.1 180.190.1.1`, "
    "`/isp 8.8.8.8,1.1.1.1`\n"
    "IPv6 works too: `/isp 2001:4860:4860::8888`"
)

# "AS4775 Globe Telecoms" / "as132199" — the ASN and the name it is announced under.
_AS_TOKEN_RE = re.compile(r"(?i)(?:^|[^a-z0-9])as[\s_-]?(\d{1,10})(?![0-9])")
# RIPEstat holders read "GLOBE-TELECOM-AS - Globe Telecoms"; keep the human half.
_HOLDER_SPLIT_RE = re.compile(r"\s+-\s+")
# Handles, not names: "GLOBE-TELECOM-AS", "CLOUDFLARENET", "GOOGLE".
_HANDLE_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]*$")

# Dropped before grouping names: legal forms and bare noise. Deliberately does
# NOT include industry words ("telecom", "networks"), which carry meaning —
# folding those would merge genuinely different companies.
_LEGAL_TOKENS = frozenset(
    """
    inc incorporated ltd limited llc llp lp plc corp corporation company co
    sa sas sarl srl spa sl spa nv bv gmbh ag kg kgaa oy oyj ab aps as asa
    pte pty sdn bhd kk jsc ooo pjsc cjsc tbk pt dba trading
    """.split()
)
_NOISE_TOKENS = frozenset({"the", "a", "an", "and", "of"})
_TRAIT_LABEL = {
    "mobile": "📱 mobile",
    "proxy": "🕵️ proxy",
    "hosting": "🏢 hosting",
    "vpn": "🔒 vpn",
    "tor": "🧅 tor",
    "abuser": "⚠️ abuser",
}
# "datacenter" and "hosting" are the same claim wearing two provider vocabularies.
_TRAIT_CANON = {"datacenter": "hosting"}


def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name) or "").strip() or default)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip() or default)
    except (TypeError, ValueError):
        return default


def _timeout() -> float:
    return max(1.0, _env_float("BOT_ISP_TIMEOUT", 7.0))


def _max_ips() -> int:
    return max(1, _env_int("BOT_ISP_MAX_IPS", 8))


def _ipinfo_token() -> str:
    return (os.getenv("BOT_ISP_IPINFO_TOKEN") or os.getenv("IPINFO_TOKEN") or "").strip()


# ---------------------------------------------------------------------------
# Per-provider records
# ---------------------------------------------------------------------------


@dataclass
class Record:
    """One provider's answer for one IP, normalised to a common shape."""

    key: str
    ok: bool = False
    error: str = ""
    # Carrier-level names (ISP, AS organisation) — these drive "ISP / Organization".
    isp: tuple[str, ...] = ()
    # Customer / netblock names ("MAKATI ENTERPRISE CUSTOMERS") — shown separately.
    netname: tuple[str, ...] = ()
    asns: tuple[int, ...] = ()
    country_name: str = ""
    country_code: str = ""
    city: str = ""
    region: str = ""
    prefix: str = ""
    domain: str = ""
    flag_emoji: str = ""
    traits: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return _PROVIDER_NAME.get(self.key, self.key)

    @property
    def index(self) -> int:
        return _PROVIDER_INDEX.get(self.key, len(_PROVIDERS))


def _clean(value: Any) -> str:
    """Trim a provider string, collapsing whitespace; '' for anything unusable."""
    if value is None or isinstance(value, bool):
        return ""
    s = re.sub(r"\s+", " ", str(value)).strip().strip(",;")
    # Providers use these as "no data" sentinels.
    if s.lower() in ("", "-", "n/a", "na", "none", "null", "unknown", "not found"):
        return ""
    return s


def _payload_msg(data: Any, *keys: str) -> str:
    """A provider's own error text, tolerating a 200 whose body is not even a
    dict — a bare string or number would blow up on ``.get``."""
    if not isinstance(data, dict):
        return ""
    for key in keys:
        value = data.get(key)
        if isinstance(value, dict):
            value = value.get("title") or value.get("message")
        text = _clean(value)
        if text:
            return text
    return ""


def _split_as(value: Any) -> tuple[tuple[int, ...], str]:
    """``'AS4775 Globe Telecoms'`` → ``((4775,), 'Globe Telecoms')``."""
    s = _clean(value)
    if not s:
        return (), ""
    asns = tuple(int(m.group(1)) for m in _AS_TOKEN_RE.finditer(s))
    name = _clean(_AS_TOKEN_RE.sub(" ", s))
    return asns, name


def _holder_name(value: Any) -> str:
    """``'GLOBE-TELECOM-AS - Globe Telecoms'`` → ``'Globe Telecoms'``."""
    s = _clean(value)
    if not s:
        return ""
    parts = [p for p in (_clean(p) for p in _HOLDER_SPLIT_RE.split(s)) if p]
    if not parts:
        return ""
    # Prefer a part that is a real name over an all-caps registry handle.
    for p in reversed(parts):
        if not _HANDLE_RE.match(p):
            return p
    return parts[-1]


def _as_int(value: Any) -> Optional[int]:
    try:
        n = int(str(value).strip().lstrip("Aa Ss"))
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _get(url: str, **kwargs: Any) -> Any:
    resp = requests.get(
        url,
        timeout=_timeout(),
        headers={"Accept": "application/json", "User-Agent": "osedutybot/isp"},
        **kwargs,
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_ipinfo(ip: str) -> Record:
    rec = Record(key="ipinfo")
    token = _ipinfo_token()
    data = _get(f"https://ipinfo.io/{ip}/json", params={"token": token} if token else None)
    if not isinstance(data, dict) or data.get("error"):
        rec.error = _payload_msg(data, "error") or "no data"
        return rec
    asns, org = _split_as(data.get("org"))
    # With a token, IPinfo returns a richer nested "asn"/"company" block instead.
    asn_block = data.get("asn") if isinstance(data.get("asn"), dict) else {}
    company = data.get("company") if isinstance(data.get("company"), dict) else {}
    extra_asn = _as_int(asn_block.get("asn"))
    rec.ok = True
    rec.asns = tuple(dict.fromkeys(asns + ((extra_asn,) if extra_asn else ())))
    rec.isp = tuple(
        dict.fromkeys(n for n in (org, _clean(asn_block.get("name")), _clean(company.get("name"))) if n)
    )
    rec.country_code = _clean(data.get("country"))
    rec.city = _clean(data.get("city"))
    rec.region = _clean(data.get("region"))
    rec.domain = _clean(asn_block.get("domain")) or _clean(company.get("domain"))
    privacy = data.get("privacy") if isinstance(data.get("privacy"), dict) else {}
    rec.traits = tuple(t for t in ("vpn", "tor", "proxy", "hosting") if privacy.get(t) is True)
    return rec


def _fetch_ipapi(ip: str) -> Record:
    """ip-api.com — free tier is plain HTTP only (HTTPS is a paid feature)."""
    rec = Record(key="ipapi")
    data = _get(
        f"http://ip-api.com/json/{ip}",
        params={
            "fields": "status,message,country,countryCode,regionName,city,isp,org,as,asname,"
            "reverse,mobile,proxy,hosting,query"
        },
    )
    if not isinstance(data, dict) or data.get("status") != "success":
        rec.error = _payload_msg(data, "message") or "no data"
        return rec
    asns, as_org = _split_as(data.get("as"))
    isp = _clean(data.get("isp"))
    org = _clean(data.get("org"))
    rec.ok = True
    rec.asns = asns
    rec.isp = tuple(dict.fromkeys(n for n in (as_org, isp) if n))
    # ip-api's "org" is the netblock holder, which is often the customer, not the carrier.
    rec.netname = tuple(n for n in (org,) if n and n.lower() != isp.lower())
    rec.country_name = _clean(data.get("country"))
    rec.country_code = _clean(data.get("countryCode"))
    rec.city = _clean(data.get("city"))
    rec.region = _clean(data.get("regionName"))
    rec.domain = _clean(data.get("reverse"))
    rec.traits = tuple(t for t in ("mobile", "proxy", "hosting") if data.get(t) is True)
    return rec


def _fetch_ipwhois(ip: str) -> Record:
    rec = Record(key="ipwhois")
    data = _get(f"https://ipwho.is/{ip}")
    if not isinstance(data, dict) or not data.get("success"):
        rec.error = _payload_msg(data, "message") or "no data"
        return rec
    conn = data.get("connection") if isinstance(data.get("connection"), dict) else {}
    flag = data.get("flag") if isinstance(data.get("flag"), dict) else {}
    asn = _as_int(conn.get("asn"))
    isp = _clean(conn.get("isp"))
    org = _clean(conn.get("org"))
    rec.ok = True
    rec.asns = (asn,) if asn else ()
    rec.isp = tuple(n for n in (isp,) if n)
    rec.netname = tuple(n for n in (org,) if n and n.lower() != isp.lower())
    rec.country_name = _clean(data.get("country"))
    rec.country_code = _clean(data.get("country_code"))
    rec.city = _clean(data.get("city"))
    rec.region = _clean(data.get("region"))
    rec.domain = _clean(conn.get("domain"))
    rec.flag_emoji = _clean(flag.get("emoji"))
    return rec


def _fetch_ipapiis(ip: str) -> Record:
    rec = Record(key="ipapiis")
    data = _get("https://api.ipapi.is/", params={"q": ip})
    if not isinstance(data, dict) or data.get("error"):
        rec.error = _payload_msg(data, "error") or "no data"
        return rec
    if data.get("is_bogon") is True:
        rec.error = "bogon range"
        return rec
    asn = _as_int(data.get("asn_num"))
    asn_block = data.get("asn") if isinstance(data.get("asn"), dict) else {}
    asn = asn or _as_int(asn_block.get("asn"))
    asn_org = _clean(data.get("asn_org")) or _clean(asn_block.get("org"))
    company = _clean(data.get("company_name"))
    if not company:
        company_block = data.get("company") if isinstance(data.get("company"), dict) else {}
        company = _clean(company_block.get("name"))
    rec.ok = True
    rec.asns = (asn,) if asn else ()
    rec.isp = tuple(n for n in (asn_org,) if n)
    rec.netname = tuple(n for n in (company,) if n and n.lower() != asn_org.lower())
    rec.country_code = _clean(data.get("cc")) or _clean(data.get("country_code"))
    rec.prefix = _clean(asn_block.get("route"))
    rec.domain = _clean(asn_block.get("domain"))
    rec.traits = tuple(
        t
        for t in ("datacenter", "vpn", "proxy", "tor", "abuser")
        if data.get(f"is_{t}") is True
    )
    return rec


def _fetch_ripe(ip: str) -> Record:
    """RIPEstat — the authoritative announced prefix and its ASN holder."""
    rec = Record(key="ripe")
    data = _get(
        "https://stat.ripe.net/data/prefix-overview/data.json",
        params={"resource": ip, "sourceapp": "osedutybot"},
    )
    payload = (data or {}).get("data") if isinstance(data, dict) else None
    if not isinstance(payload, dict) or not payload.get("announced"):
        rec.error = "not announced"
        return rec
    asns: list[int] = []
    names: list[str] = []
    for entry in payload.get("asns") or []:
        if not isinstance(entry, dict):
            continue
        n = _as_int(entry.get("asn"))
        if n:
            asns.append(n)
        holder = _holder_name(entry.get("holder"))
        if holder:
            names.append(holder)
    rec.ok = bool(asns or names)
    rec.asns = tuple(dict.fromkeys(asns))
    rec.isp = tuple(dict.fromkeys(names))
    rec.prefix = _clean(payload.get("resource"))
    if not rec.ok:
        rec.error = "no ASN data"
    return rec


def _fetch_iplocation(ip: str) -> Record:
    rec = Record(key="iplocation")
    data = _get("https://api.iplocation.net/", params={"ip": ip})
    if not isinstance(data, dict) or str(data.get("response_code")) != "200":
        rec.error = _payload_msg(data, "response_message") or "no data"
        return rec
    isp = _clean(data.get("isp"))
    rec.ok = True
    # iplocation reports the netblock holder under "isp"; treat it as a netname
    # so a customer name never outvotes the carrier on the ISP line.
    rec.netname = tuple(n for n in (isp,) if n)
    rec.country_name = _clean(data.get("country_name"))
    rec.country_code = _clean(data.get("country_code2"))
    return rec


_FETCHERS = {
    "ipinfo": _fetch_ipinfo,
    "ipapi": _fetch_ipapi,
    "ipwhois": _fetch_ipwhois,
    "ipapiis": _fetch_ipapiis,
    "ripe": _fetch_ripe,
    "iplocation": _fetch_iplocation,
}


# ---------------------------------------------------------------------------
# Name grouping
# ---------------------------------------------------------------------------


def _norm_key(text: str) -> str:
    """Grouping key: 'Globe Telecom Inc.', 'Globe Telecoms' and
    'Globe Telecom (GMCR,INC)' all collapse to 'globe telecom'."""
    s = re.sub(r"\(.*?\)", " ", text or "")  # parenthetical noise
    s = re.sub(r"(?i)\bAS\d{1,10}\b", " ", s)  # stray ASN tokens
    # Fold diacritics FIRST. The ASCII strip below turns every accented letter
    # into a space, so without this "Orange Côte d'Ivoire" keys as
    # 'orange c te d ivoire' and never groups with 'ORANGE COTE D'IVOIRE'.
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^0-9A-Za-z]+", " ", s).lower()
    out: list[str] = []
    for tok in s.split():
        if tok in _LEGAL_TOKENS or tok in _NOISE_TOKENS:
            continue
        # Fold trailing plurals: "telecoms" == "telecom", "networks" == "network".
        if len(tok) > 4 and tok.endswith("s") and not tok.endswith("ss"):
            tok = tok[:-1]
        out.append(tok)
    return " ".join(out) or re.sub(r"\s+", " ", (text or "").strip().lower())


# Industry words too generic to justify absorbing one group into another on
# their own — "telecom" alone must not pull "Globe Telecom" and "PLDT Telecom"
# together.
_GENERIC_TOKENS = frozenset(
    """
    telecom telecomunicacione telecommunication network networking communication
    internet broadband mobile wireless cable fiber data hosting cloud digital
    service solution system technology media online global group
    """.split()
)


def _is_generic(toks: set[str]) -> bool:
    """A lone industry word, too weak to host an absorption on its own."""
    return len(toks) == 1 and toks <= _GENERIC_TOKENS


def _absorb_subsets(
    groups: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]]]:
    """Fold a name into another that contains all its words, so 'Datacamp
    Limited' and 'CDN77 Datacamp Limited' — or 'X-DSL Networking Solutions' and
    'Networking Solutions - X-DSL Networking Solutions' — read as one company
    instead of two.

    Hosts are *chosen* shortest-first (the extra words are almost always a brand
    or netblock qualifier) but the result is *emitted* in the order the callers'
    groups were first seen, so the card's ordering follows the data rather than
    name length. Returns the folded groups plus, per survivor, the keys folded
    into it — callers need those to suppress a name on a later line."""
    host_of: dict[str, str] = {}
    hosts: list[str] = []
    for key in sorted(groups, key=lambda k: (len(k.split()), len(k))):
        toks = set(key.split())
        if _is_generic(toks):
            host_of[key] = key
            hosts.append(key)
            continue
        host = next(
            (
                hk
                for hk in hosts
                if not _is_generic(set(hk.split()))
                and (set(hk.split()) <= toks or toks <= set(hk.split()))
            ),
            None,
        )
        host_of[key] = host or key
        if host is None:
            hosts.append(key)

    out: dict[str, dict[str, Any]] = {}
    absorbed: dict[str, set[str]] = {}
    for key in groups:  # insertion order — first-seen wins the slot
        host = host_of[key]
        if host not in out:
            out[host] = dict(groups[host])
            absorbed[host] = set()
        if key == host:
            continue
        out[host]["providers"] = out[host]["providers"] | groups[key]["providers"]
        absorbed[host].add(key)
        # A shorter key is not automatically the better label: "Globe" must not
        # beat "Globe Telecom Inc.". Re-rank the two candidates.
        if groups[key]["best_score"] > out[host]["best_score"]:
            out[host]["best"] = groups[key]["best"]
            out[host]["best_score"] = groups[key]["best_score"]
    return out, absorbed


def _has_legal_suffix(text: str) -> bool:
    """A legal form *outside* parentheses — '(GMCR,INC)' does not count."""
    bare = re.sub(r"\(.*?\)", " ", text or "")
    return any(tok in _LEGAL_TOKENS for tok in re.split(r"[^0-9A-Za-z]+", bare.lower()) if tok)


def _display_score(text: str, provider_index: int) -> tuple:
    """Rank variants of the same name; highest wins. Prefers the most official
    looking form, which is what turns five Globe spellings into
    'Globe Telecom Inc.' rather than 'GLOBE-TELECOM-AS'."""
    return (
        1 if _has_legal_suffix(text) else 0,
        0 if _HANDLE_RE.match(text) else 1,  # registry handles last
        1 if re.search(r"[a-z]", text) else 0,  # mixed case over SHOUTING
        1 if "(" not in text else 0,
        -len(text.split()),  # fewer words: drops brand/netblock qualifiers
        -provider_index,  # higher-priority provider breaks ties
        -len(text),  # then the tighter spelling
    )


@dataclass
class Merged:
    """One rendered card line: its value, its provenance chip, what was hidden."""

    value: str = ""
    chip: str = ""
    hidden: int = 0
    sources: tuple[str, ...] = ()
    # Normalised keys of the groups shown, so a later line can skip repeats.
    keys: frozenset[str] = frozenset()

    def __bool__(self) -> bool:
        return bool(self.value)


def _chip_for(provider_keys: set[str]) -> tuple[str, tuple[str, ...]]:
    if not provider_keys:
        return "", ()
    ordered = sorted(provider_keys, key=lambda k: _PROVIDER_INDEX.get(k, len(_PROVIDERS)))
    names = tuple(_PROVIDER_NAME.get(k, k) for k in ordered)
    chip = names[0] if len(names) == 1 else f"{names[0]} +{len(names) - 1}"
    return chip, names


def _merge_names(
    cands: list[tuple[str, str]],
    *,
    max_groups: int = 2,
    exclude: frozenset[str] = frozenset(),
) -> Merged:
    """``cands`` are ``(text, provider_key)`` pairs. Groups equivalent spellings,
    orders groups by support, and joins the survivors with ' / '. ``exclude``
    holds keys already shown on another line; a candidate is dropped when its
    words match, contain, or are contained by any of them — plain string
    equality would miss exactly the near-spellings this module exists to fold."""
    excluded_tokens = [set(k.split()) for k in exclude if k]
    groups: dict[str, dict[str, Any]] = {}
    for text, pkey in cands:
        text = _clean(text)
        if not text:
            continue
        key = _norm_key(text)
        if not key:
            continue
        toks = set(key.split())
        if any(
            ex <= toks or toks <= ex for ex in excluded_tokens if not _is_generic(ex)
        ):
            continue
        g = groups.setdefault(
            key, {"providers": set(), "best": text, "best_score": None, "seq": len(groups)}
        )
        g["providers"].add(pkey)
        score = _display_score(text, _PROVIDER_INDEX.get(pkey, len(_PROVIDERS)))
        if g["best_score"] is None or score > g["best_score"]:
            g["best"], g["best_score"] = text, score
    if not groups:
        return Merged()
    groups, absorbed = _absorb_subsets(groups)
    ordered = sorted(
        groups.items(),
        key=lambda kv: (
            -len(kv[1]["providers"]),
            min(_PROVIDER_INDEX.get(p, len(_PROVIDERS)) for p in kv[1]["providers"]),
            kv[1]["seq"],  # deterministic: ties keep first-seen order
        ),
    )
    shown, hidden = ordered[:max_groups], ordered[max_groups:]
    used: set[str] = set()
    keys: set[str] = set()
    for key, g in shown:
        used |= g["providers"]
        keys.add(key)
        keys |= absorbed.get(key, set())  # folded spellings must exclude too
    chip, names = _chip_for(used)
    return Merged(
        value=" / ".join(g["best"] for _key, g in shown),
        chip=chip,
        hidden=len(hidden),
        sources=names,
        keys=frozenset(keys),
    )


def _merge_asns(cands: list[tuple[int, str]]) -> Merged:
    """ASNs group on the number itself — no spelling to reconcile. All distinct
    ASNs are shown (this is what renders 'AS4775 / AS132199')."""
    groups: dict[int, set[str]] = {}
    for asn, pkey in cands:
        if asn:
            groups.setdefault(asn, set()).add(pkey)
    if not groups:
        return Merged()
    used: set[str] = set()
    for providers in groups.values():
        used |= providers
    chip, names = _chip_for(used)
    return Merged(
        value=" / ".join(f"AS{n}" for n in sorted(groups)),
        chip=chip,
        sources=names,
    )


def _merge_country(recs: list[Record]) -> Merged:
    """Countries get their own grouping, not ``_merge_names``: they are a closed
    set with no brand qualifiers to absorb, so the subset fold there would eat a
    real answer ('Guinea' is not a spelling of 'Equatorial Guinea', 'Sudan' is
    not 'South Sudan'). Groups key on the ISO code, learned from the providers
    that return both a code and a name."""
    code_to_name: dict[str, str] = {}
    for rec in recs:
        code, name = rec.country_code.upper(), rec.country_name
        if code and name and code not in code_to_name:
            code_to_name[code] = name
    emoji = next((rec.flag_emoji for rec in recs if rec.flag_emoji), "")

    groups: dict[str, dict[str, Any]] = {}
    for rec in recs:
        code = rec.country_code.upper()
        label = code_to_name.get(code) or rec.country_name or code
        if not label:
            continue
        key = code or _norm_key(label)
        group = groups.setdefault(
            key, {"providers": set(), "label": label, "seq": len(groups)}
        )
        group["providers"].add(rec.key)
        if len(label) > len(group["label"]):  # a full name beats a bare code
            group["label"] = label
    if not groups:
        return Merged()

    ordered = sorted(
        groups.items(),
        key=lambda kv: (
            -len(kv[1]["providers"]),
            min(_PROVIDER_INDEX.get(p, len(_PROVIDERS)) for p in kv[1]["providers"]),
            kv[1]["seq"],
        ),
    )
    # Keep only answers with the leading level of support. One provider
    # geolocating an anycast address to a different country is noise, not a
    # second country — and it must not be counted as "(+1 more)" either, since
    # that reads as a truncated list rather than a discarded outlier.
    top = len(ordered[0][1]["providers"])
    shown = [kv for kv in ordered if len(kv[1]["providers"]) == top]
    used: set[str] = set()
    for _key, group in shown:
        used |= group["providers"]
    chip, names = _chip_for(used)
    value = " / ".join(group["label"] for _key, group in shown)
    if emoji and len(shown) == 1:
        value = f"{emoji} {value}"
    return Merged(
        value=value,
        chip=chip,
        sources=names,
        keys=frozenset(key for key, _group in shown),
    )


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------


@dataclass
class IpResult:
    ip: str
    records: list[Record] = field(default_factory=list)
    note: str = ""  # set instead of querying, e.g. "private range"

    @property
    def good(self) -> list[Record]:
        return [r for r in self.records if r.ok]

    @property
    def resolved(self) -> bool:
        return bool(self.good)

    @property
    def isp(self) -> Merged:
        return _merge_names([(n, r.key) for r in self.good for n in r.isp])

    @property
    def netname(self) -> Merged:
        # Never repeat a name the ISP line already shows.
        return _merge_names(
            [(n, r.key) for r in self.good for n in r.netname], exclude=self.isp.keys
        )

    @property
    def asn(self) -> Merged:
        return _merge_asns([(n, r.key) for r in self.good for n in r.asns])

    @property
    def country(self) -> Merged:
        return _merge_country(self.good)

    @property
    def city_region(self) -> str:
        parts = _merge_names(
            [(f"{r.city}, {r.region}".strip(" ,"), r.key) for r in self.good if r.city or r.region],
            max_groups=1,
        )
        return parts.value

    @property
    def prefix(self) -> str:
        return _merge_names([(r.prefix, r.key) for r in self.good if r.prefix], max_groups=1).value

    @property
    def domain(self) -> str:
        return _merge_names([(r.domain, r.key) for r in self.good if r.domain], max_groups=1).value

    @property
    def traits(self) -> list[str]:
        seen: list[str] = []
        for rec in self.good:
            for raw in rec.traits:
                t = _TRAIT_CANON.get(raw, raw)
                if t not in seen:
                    seen.append(t)
        return seen

    @property
    def failures(self) -> list[str]:
        return [f"{r.name}: {r.error}" for r in self.records if not r.ok and r.error]


def parse_ips(query: str) -> tuple[list[str], list[str]]:
    """Split the text after ``/isp`` into (valid IPs, rejected tokens). Accepts
    space, comma, semicolon and newline separators, and tolerates the wrapping
    braces/brackets people copy from a usage line."""
    tokens = [t for t in re.split(r"[\s,;]+", (query or "").strip()) if t]
    ips: list[str] = []
    bad: list[str] = []
    for tok in tokens:
        cleaned = tok.strip("{}()<>\"'`").rstrip(".")
        if not cleaned:
            continue
        # "[2001:db8::1]:443" / "[2001:db8::1]" — the bracketed form must be
        # unwrapped before the host:port rule, which only understands IPv4.
        bracketed = re.match(r"^\[([0-9A-Fa-f:.]+)\](?::\d{1,5})?$", cleaned)
        if bracketed:
            cleaned = bracketed.group(1)
        cleaned = cleaned.strip("[]")
        # Tolerate "1.2.3.4/24" and "8.8.4.4:53" style pastes.
        cleaned = re.sub(r"/\d{1,3}$", "", cleaned)
        if cleaned.count(":") == 1 and "." in cleaned:
            cleaned = cleaned.rsplit(":", 1)[0]  # host:port
        try:
            addr = ipaddress.ip_address(cleaned)
        except ValueError:
            bad.append(tok)
            continue
        text = addr.compressed
        if text not in ips:
            ips.append(text)
    return ips, bad


def _reserved_note(ip: str) -> str:
    """Why a non-public address has no ISP, without spending a request on it."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return ""
    # Most specific first: link-local and loopback are also is_private in
    # Python's model, so testing is_private early would mislabel them.
    if addr.is_unspecified:
        return "unspecified address"
    if addr.is_loopback:
        return "loopback address"
    if addr.is_link_local:
        return "link-local address"
    if addr.is_multicast:
        return "multicast address"
    if addr.is_reserved:
        return "reserved range"
    if addr.is_private:
        return "private range (RFC 1918 / ULA)"
    return ""


def lookup(ips: list[str]) -> list[IpResult]:
    """Query every provider for every IP in parallel, isolating each failure."""
    results = {ip: IpResult(ip=ip) for ip in ips}
    units: list[tuple[str, str]] = []
    for ip in ips:
        note = _reserved_note(ip)
        if note:
            results[ip].note = note
            continue
        units.extend((ip, pkey) for pkey, _name in _PROVIDERS)
    if units:
        workers = min(len(units), max(4, _env_int("BOT_ISP_WORKERS", 12)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="isp-lookup") as ex:
            futures = {ex.submit(_FETCHERS[pkey], ip): (ip, pkey) for ip, pkey in units}
            for fut in as_completed(futures):
                ip, pkey = futures[fut]
                try:
                    results[ip].records.append(fut.result())
                except Exception as ex_err:  # noqa: BLE001 — one dead provider must not sink the card
                    msg = re.sub(r"\s+", " ", str(ex_err))[:120] or type(ex_err).__name__
                    results[ip].records.append(Record(key=pkey, error=msg))
                    print(f"[isp] {pkey} failed for {ip}: {msg}", flush=True)
    for res in results.values():
        res.records.sort(key=lambda r: r.index)
    return [results[ip] for ip in ips]


# ---------------------------------------------------------------------------
# Aggregation across every IP in the command
# ---------------------------------------------------------------------------


@dataclass
class Summary:
    isp: Merged
    country: Merged
    asn: Merged
    netname: Merged
    prefixes: list[str]
    traits: list[str]
    sources: list[str]


def summarise(results: list[IpResult]) -> Summary:
    good = [r for res in results for r in res.good]
    prefixes: list[str] = []
    traits: list[str] = []
    for res in results:
        if res.prefix and res.prefix not in prefixes:
            prefixes.append(res.prefix)
        for t in res.traits:
            if t not in traits:
                traits.append(t)
    # One IP has one carrier, so two slots is plenty; a multi-IP command
    # legitimately spans several, and hiding them all behind "(+N more)" would
    # make the summary less useful than the per-IP block below it.
    slots = 2 if len(results) <= 1 else min(5, 1 + len(results))
    isp = _merge_names([(n, r.key) for r in good for n in r.isp], max_groups=slots)
    return Summary(
        isp=isp,
        country=_merge_country(good),
        asn=_merge_asns([(n, r.key) for r in good for n in r.asns]),
        netname=_merge_names(
            [(n, r.key) for r in good for n in r.netname],
            max_groups=slots,
            exclude=isp.keys,
        ),
        prefixes=prefixes,
        traits=traits,
        sources=sorted({r.name for r in good}, key=lambda n: list(_PROVIDER_NAME.values()).index(n)),
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_MD_ESCAPES = (
    ("&", "&amp;"),
    ("<", "&lt;"),
    (">", "&gt;"),
    ("*", "&#42;"),
    ("_", "&#95;"),
    ("~", "&#126;"),
    ("`", "&#96;"),
    ("[", "&#91;"),
    ("]", "&#93;"),
)


def _md(text: Any) -> str:
    """Escape provider text so it cannot break out into card markdown."""
    out = str(text if text is not None else "")
    for raw, esc in _MD_ESCAPES:
        out = out.replace(raw, esc)
    return out


def _chips_enabled() -> bool:
    """Inline ``<text_tag>`` chips are documented for the card-v2 ``markdown``
    component but have no precedent in this repo, and a tenant that accepts the
    card without interpreting them would show the raw markup — which no
    API-level fallback can catch. ``BOT_ISP_CARD_CHIPS=0`` switches every chip to
    the plain ``[IPinfo +2]`` bracket form on proven ``div``/``lark_md``."""
    return (os.getenv("BOT_ISP_CARD_CHIPS") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _tag(text: str, color: str = "blue") -> str:
    """Inline badge — card JSON v2 ``<text_tag>``, e.g. ``IPinfo +2``."""
    if not text:
        return ""
    if not _chips_enabled():
        return f"`{_md(text)}`"
    return f"<text_tag color='{color}'>{_md(text)}</text_tag>"


def _text_element(content: str) -> dict[str, Any]:
    """A body text block: the v2 ``markdown`` component when chips are on (only
    that component interprets ``<text_tag>``), else the repo-standard
    ``div``/``lark_md`` pair used by all 65 other schema-2.0 cards here."""
    if _chips_enabled():
        return {"tag": "markdown", "content": content}
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _bullet(label: str, merged: Merged) -> str:
    value = _md(merged.value)
    if merged.hidden:
        value += f" _(+{merged.hidden} more)_"
    chip = f" {_tag(merged.chip)}" if merged.chip else ""
    return f"- **{_md(label)}:** {value}{chip}"


def _plain_bullet(label: str, merged: Merged) -> str:
    extra = f" (+{merged.hidden} more)" if merged.hidden else ""
    chip = f"  [{merged.chip}]" if merged.chip else ""
    return f"• {label}: {merged.value}{extra}{chip}"


def _per_ip_lines(res: IpResult) -> list[str]:
    """One compact line per IP so a multi-IP command stays unambiguous."""
    if res.note:
        return [f"`{_md(res.ip)}` — _{_md(res.note)}_"]
    if not res.resolved:
        why = "; ".join(res.failures[:2]) or "no provider returned data"
        return [f"`{_md(res.ip)}` — ❔ _{_md(why)}_"]
    bits = [b for b in (res.isp.value, res.asn.value, res.country.value) if b]
    detail = [b for b in (res.city_region, res.prefix, res.domain) if b]
    lines = [f"`{_md(res.ip)}` — {_md(' · '.join(bits))}"]
    if detail:
        lines.append(f"  ↳ {_md(' · '.join(detail))}")
    if res.traits:
        lines.append("  ↳ " + " ".join(_md(_TRAIT_LABEL.get(t, t)) for t in res.traits))
    return lines


def build_card(results: list[IpResult], *, elapsed: float = 0.0) -> dict[str, Any]:
    """Lark card JSON v2 — ``markdown`` components, so inline ``<text_tag>``
    chips render as real badges."""
    summary = summarise(results)
    resolved = [res for res in results if res.resolved]
    elements: list[dict[str, Any]] = []

    # The queried IPs go in the body, not header.subtitle: bot_help._card_shell
    # takes a subtitle and renders it exactly this way, and header.subtitle has
    # no precedent in this repo.
    queried = ", ".join(res.ip for res in results)
    if len(queried) > 120:
        queried = f"{queried[:117]}…"
    elements.append(_text_element(f"**Queried:** `{_md(queried)}`"))

    details = ["**IP Details**"]
    if summary.isp:
        details.append(_bullet("ISP / Organization", summary.isp))
    if summary.country:
        details.append(_bullet("Country", summary.country))
    if summary.asn:
        details.append(_bullet("ASN", summary.asn))
    if len(details) == 1:
        details.append("_No public ISP data returned for the IP(s) above._")
    elements.append(_text_element("\n".join(details)))

    aside = []
    if summary.netname:
        aside.append(_bullet("Netblock / Customer", summary.netname))
    if summary.prefixes:
        aside.append(f"- **Prefix:** {_md(' / '.join(summary.prefixes[:4]))}")
    if summary.traits:
        aside.append(
            "- **Flags:** " + " ".join(_tag(_TRAIT_LABEL.get(t, t), "orange") for t in summary.traits)
        )
    if aside:
        elements.append(_text_element("\n".join(aside)))

    # With a single resolved IP the summary already says everything a per-IP
    # block would repeat.
    if len(results) > 1 or len(resolved) != len(results):
        elements.append({"tag": "hr"})
        per_ip = [f"**Per IP** ({len(resolved)}/{len(results)} resolved)"]
        for res in results:
            per_ip.extend(_per_ip_lines(res))
        elements.append(_text_element("\n".join(per_ip)))

    # Card JSON v2 has no "note" component — its component list is div / markdown
    # / hr / img / … — so the sources footer is an italic text block instead.
    note = f"🔎 {_md(', '.join(summary.sources) or 'no source responded')}"
    if elapsed:
        note += f" · {elapsed:.1f}s"
    elements.append(_text_element(f"_{note}_"))

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "blue" if resolved else "grey",
            "title": {"tag": "plain_text", "content": "🌐 IP / ISP lookup"},
        },
        "body": {"elements": elements},
    }


def build_text(results: list[IpResult], *, elapsed: float = 0.0) -> str:
    """Plain-text mirror of the card, for when the interactive send is rejected."""
    summary = summarise(results)
    lines = ["🌐 IP Details"]
    if summary.isp:
        lines.append(_plain_bullet("ISP / Organization", summary.isp))
    if summary.country:
        lines.append(_plain_bullet("Country", summary.country))
    if summary.asn:
        lines.append(_plain_bullet("ASN", summary.asn))
    if len(lines) == 1:
        lines.append("• No public ISP data returned.")
    if summary.netname:
        lines.append(_plain_bullet("Netblock / Customer", summary.netname))
    if summary.prefixes:
        lines.append(f"• Prefix: {' / '.join(summary.prefixes[:4])}")
    if summary.traits:
        lines.append("• Flags: " + ", ".join(_TRAIT_LABEL.get(t, t) for t in summary.traits))
    if len(results) > 1 or any(not res.resolved for res in results):
        lines.append("")
        for res in results:
            if res.note:
                lines.append(f"• {res.ip} — {res.note}")
            elif not res.resolved:
                lines.append(f"• {res.ip} — no provider returned data")
            else:
                bits = [b for b in (res.isp.value, res.asn.value, res.country.value) if b]
                lines.append(f"• {res.ip} — {' · '.join(bits)}")
    tail = ", ".join(summary.sources) or "no source responded"
    if elapsed:
        tail += f" · {elapsed:.1f}s"
    lines.append("")
    lines.append(f"🔎 {tail}")
    return "\n".join(lines)


def handle_isp_command(query: str) -> tuple[Optional[dict[str, Any]], str]:
    """``/isp`` entry point. Returns ``(card, text)``; ``card`` is None for usage
    and argument errors, where ``text`` is the message to send as-is."""
    ips, bad = parse_ips(query)
    if not ips:
        extra = f"\n⚠️ Not an IP address: {', '.join(bad[:5])}" if bad else ""
        return None, USAGE + extra
    limit = _max_ips()
    dropped = ips[limit:]
    ips = ips[:limit]
    started = time.monotonic()
    results = lookup(ips)
    elapsed = time.monotonic() - started
    card = build_card(results, elapsed=elapsed)
    text = build_text(results, elapsed=elapsed)
    warnings = []
    # Natural-language routing ("isp lookup 8.8.8.8") hands the prose through as
    # arguments, so once at least one IP parsed, only complain about tokens that
    # were plainly *meant* to be addresses — never about ordinary words.
    malformed = [t for t in bad if any(c.isdigit() for c in t) and ("." in t or ":" in t)]
    if malformed:
        warnings.append(f"⚠️ Skipped (not a valid IP): {', '.join(malformed[:5])}")
    if dropped:
        warnings.append(f"⚠️ Only the first {limit} IPs were checked; skipped {len(dropped)} more.")
    if warnings:
        joined = "\n".join(warnings)
        card["body"]["elements"].append(_text_element("\n".join(_md(w) for w in warnings)))
        text = f"{text}\n\n{joined}"
    return card, text


if __name__ == "__main__":  # manual check: python ipisp.py 112.198.1.1 203.177.42.1
    import json
    import sys

    _card, _text = handle_isp_command(" ".join(sys.argv[1:]) or "112.198.1.1 203.177.42.1 180.190.1.1")
    print(_text)
    print()
    print(json.dumps(_card, ensure_ascii=False, indent=2) if _card else "(no card)")
