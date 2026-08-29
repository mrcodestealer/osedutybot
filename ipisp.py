#!/usr/bin/env python3
"""
``/isp <ip> [ip ...]`` — ISP / Organization, Country and ASN for one or more IPs.

The card answers one question — "where is this player" — in three lines: what
the address is, where it is, and who runs it::

    🏠 Real connection — user is in 🇵🇭 Philippines
    112.198.1.1
    • Location: 🇵🇭 Philippines · Quezon City   [IPinfo +2]
    • Operator: Globe Telecoms                 [IPinfo +4]
    🔎 AS4775 · 112.198.0.0/16 · 6/6 sources

    🔒 VPN detected — the user's real location is UNKNOWN
    217.145.74.157
    • VPN server: 🇵🇭 Philippines · Makati City [ip-api +2]
    • Operator: GSL Networks Pty LTD           [IPinfo +4]
    🔎 AS137409 · 217.145.74.0/24 · 6/6 sources

    🏢 Datacenter IP — not a home connection, real location UNKNOWN
    2001:4860:4860::8888
    • Server: 🇺🇸 United States · Mountain View [IPinfo +1] / 🇨🇦 Canada · Montreal [ip-api +1]
      ↳ both are guesses at this one datacenter server — an address here does
        not locate a person
    • Operator: Google LLC                     [IPinfo +4]
    🔎 AS15169 · 2001:4860::/32 · 6/6 sources

Three rules there earn their keep. When the address is a VPN, proxy, Tor node or
datacenter IP the headline says the location is unknown and the place is
labelled as the *server*: an earlier card showed a bare "Location: Singapore"
for a relay and it was read as the player being in Singapore. And the place is
taken whole from one provider rather than assembled from the best-supported
country plus somebody else's city — that mixing is how a card came to show
"Philippines" beside "Singapore, Singapore". A place chip therefore counts only
the sources backing the line as written: five providers may agree on Philippines
while naming five different cities, and "[IPinfo +4]" beside one of those cities
would borrow the Operator line's vocabulary — where it does mean five sources
returned the same string — to dress one source's guess as a consensus.

The third is that when the sources split on the country, every answer rides on
that one place line under that one label, best-supported first. They used to sit
on two bullets, the second of them called "Registered in" — a label that never
read whois at all, it returned the majority vote across the same geolocation
providers as the line above it. An operator read the pair as two facts about two
places (the server over here, the player over there) and concluded the card had
located the human. It had not; nothing in this module can see behind a VPN. The
real registration for that block is GB (RIPE rir-geo, the /20) and VG (the RDAP
/24 object) — neither country the bullet ever printed, so the label was not
merely vague, it named a fact this module never fetched. Which answer leads is
decided by how many providers back it, not by their display order: for
217.145.74.157 three sources said Philippines while IPinfo alone said Singapore,
then Australia an hour later for the same address, and the card headlined the
one answer that kept moving while the player's own VPN client said Philippines.

ASN and prefix ride in the footer for whoever files an abuse report. Netname,
per-flag prose and provider-by-provider disagreement are deliberately not shown;
they pushed the answer off the card. Nor is ipapi.is's ``is_abuser`` verdict: it
is a bare boolean with no evidence attached, it is true for every major public
DNS resolver, and on an address already flagged VPN or datacenter it restated
that same fact as though it were a second one.

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
    "IPv6 works too: `/isp 2001:4860:4860::8888`\n"
    "Pasting from a log? A player id next to an address is paired with it — "
    "keep each pair on its own line or block, in either order."
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


def _flag(code: str) -> str:
    """Flag emoji built from an ISO-3166 alpha-2 code via regional indicators.
    Derived, never taken from a provider, so the flag cannot disagree with the
    country name standing next to it."""
    code = (code or "").strip().upper()
    if len(code) != 2 or not code.isalpha():
        return ""
    return "".join(chr(0x1F1E6 + ord(ch) - ord("A")) for ch in code)
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
        for t in ("datacenter", "vpn", "proxy", "tor")
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
    # The flag is derived from the ISO code of the answer that actually won the
    # vote, not from whichever provider returned an emoji first. That mismatch
    # is how a card came to read "🇸🇬 Philippines", which no reader could parse.
    value = " / ".join(f"{_flag(key)} {group['label']}".strip() for key, group in shown)
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
    players: list[str] = field(default_factory=list)  # ids pasted with this IP

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

    # Rendered nowhere since "Registered in" was deleted — summarise() computes
    # its own. Do not wire a displayed country back to this: a vote winner has
    # no city to travel with, which is how "Philippines" ended up beside
    # "Singapore, Singapore". Displayed places come from _places_for(), which
    # keeps each answer whole and one provider's own.
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


# Zero-width and bidi controls. Lark turns anything IP-shaped into a hyperlink,
# and the round-trip through its rich-text editor can leave one of these glued
# to the address — invisible in the client, fatal to ip_address().
_INVISIBLE_RE = re.compile(r"[­​-‏‪-‮⁠⁦-⁩﻿]")
# "[138.84.76.76](http://138.84.76.76)" — a linkified address pasted as markdown.
_MD_LINK_RE = re.compile(r"^\[([^\]]*)\]\((.*)\)$")
# Bracketed IPv6, optionally with a port: "[2001:db8::1]:443".
_BRACKET_V6_RE = re.compile(r"^\[([0-9A-Fa-f:.]+)\](?::\d{1,5})?$")
_SCHEME_RE = re.compile(r"(?i)^[a-z][a-z0-9+.-]*://")


def _strip_ip_token(tok: str) -> str:
    """Reduce one whitespace-delimited token to a bare address.

    Written against what Lark actually delivers rather than what users type: it
    auto-links every IP, so the same address can arrive bare, as
    ``http://1.2.3.4``, as ``[1.2.3.4](http://1.2.3.4)``, as ``<http://1.2.3.4>``,
    with a trailing slash, or with an invisible zero-width character attached."""
    s = _INVISIBLE_RE.sub("", tok).strip()
    link = _MD_LINK_RE.match(s)
    if link:  # prefer the label, fall back to the href
        s = link.group(1).strip() or link.group(2).strip()
        s = _INVISIBLE_RE.sub("", s)
    s = s.strip("{}()<>\"'`,;!")
    s = _SCHEME_RE.sub("", s)  # http://, https://, even ftp://
    bracket = _BRACKET_V6_RE.match(s)
    if bracket:
        # Unwrap before the host:port rule below, which only knows IPv4.
        return bracket.group(1)
    s = s.strip("[]")
    s = s.split("/", 1)[0]  # path or CIDR suffix
    s = s.split("?", 1)[0].split("#", 1)[0]
    if s.count(":") == 1 and "." in s:
        s = s.rsplit(":", 1)[0]  # IPv4 host:port
    return s.strip().strip(".,;")


def _visible(tok: str) -> str:
    """Render a rejected token so invisible characters are actually visible —
    otherwise a zero-width space reads as a perfectly good IP in the error."""
    out = "".join(
        f"\\u{ord(c):04x}" if _INVISIBLE_RE.match(c) or ord(c) < 32 else c for c in tok
    )
    return out or "(empty)"


# A bare run of digits alongside addresses is a player id pasted from a log,
# not an address in integer notation. Long enough not to catch stray numbers.
_PLAYER_ID_RE = re.compile(r"^\d{5,15}$")


def parse_ips(query: str) -> tuple[list[str], list[str]]:
    """Split the text after ``/isp`` into (valid IPs, rejected tokens). Accepts
    space, comma, semicolon and newline separators, and tolerates the many shapes
    a Lark-linkified address arrives in — see :func:`_strip_ip_token`."""
    ips, _players, bad = parse_ip_players(query)
    return ips, bad


def parse_ip_players(query: str) -> tuple[list[str], dict[str, list[str]], list[str]]:
    """Parse ``/isp`` arguments into (IPs, player ids per IP, rejected tokens).

    Operators paste straight out of a log, so an address usually arrives with a
    player id attached — and not always in that order::

        138.84.76.76        103.40.2.176        1075487320
        1081561491          1217238182          103.40.2.142

    Pairing is therefore positional *within each blank-line separated block*,
    which is what the paste above actually encodes: block three still pairs
    correctly even though the id comes first. With everything on one line the
    whole argument is a single block and pairing is positional over it, which
    lands the same way for alternating input."""
    blocks = [b for b in re.split(r"\n\s*\n", (query or "").strip()) if b.strip()]
    ips: list[str] = []
    players: dict[str, list[str]] = {}
    bad: list[str] = []
    for block in blocks or [""]:
        block_ips: list[str] = []
        block_players: list[str] = []
        for tok in (t for t in re.split(r"[\s,;]+", block.strip()) if t):
            cleaned = _strip_ip_token(tok)
            if not cleaned:
                continue
            if _PLAYER_ID_RE.match(cleaned):
                block_players.append(cleaned)
                continue
            try:
                addr = ipaddress.ip_address(cleaned)
            except ValueError:
                bad.append(_visible(tok))
                continue
            text = addr.compressed
            block_ips.append(text)
            if text not in ips:
                ips.append(text)
        for i, ip in enumerate(block_ips):
            # Extra ids beyond the address count pile onto the last address
            # rather than being dropped silently.
            mine = block_players[i::len(block_ips)] if block_ips else []
            for pid in mine:
                if pid not in players.setdefault(ip, []):
                    players[ip].append(pid)
        if block_players and not block_ips:
            bad.extend(block_players)
    return ips, players, bad


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


def lookup(
    ips: list[str], players: Optional[dict[str, list[str]]] = None
) -> list[IpResult]:
    """Query every provider for every IP in parallel, isolating each failure."""
    players = players or {}
    results = {ip: IpResult(ip=ip, players=list(players.get(ip, ()))) for ip in ips}
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
    # One IP has one carrier and the card shows one name for it; a multi-IP
    # command legitimately spans several, and hiding them all behind "(+N more)"
    # would make the summary less useful than the per-IP lines below it.
    slots = 1 if len(results) <= 1 else min(5, 1 + len(results))
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



# Competing places join with " / " — the separator _merge_names, _merge_asns and
# _merge_country already use — but glued to the flag that follows it with a
# NO-BREAK SPACE. config.width_mode is "fill" and the place line is the longest
# on the card, so it wraps. If the break lands after the slash, the next visual
# line opens with a bare "🇸🇬 Singapore" and the "VPN server:" label that was
# supposed to govern BOTH countries is no longer in front of the second one.
# That free-standing country claim is the whole bug this change removes, and a
# wrap must not be able to put it back.
_PLACE_JOIN = " /\u00a0"  # the space after "/" is a NO-BREAK SPACE

# The short form of the dispute wording, for the multi-IP block, which has no
# room for the sentence _Kind carries. Both forms must keep saying "disagree":
# any phrasing that implies the address moved from one country to the other is
# the misreading this exists to stop.
_DISPUTE_SHORT = "sources disagree on this address"

_SUSPECT = ("vpn", "proxy", "tor", "hosting")


def _bullet(label: str, merged: Merged, *, show_hidden: bool = True) -> str:
    value = _md(merged.value)
    if merged.hidden and show_hidden:
        value += f" _(+{merged.hidden} more)_"
    chip = f" {_tag(merged.chip)}" if merged.chip else ""
    return f"- **{_md(label)}:** {value}{chip}"


def _plain_bullet(label: str, merged: Merged, *, show_hidden: bool = True) -> str:
    extra = f" (+{merged.hidden} more)" if merged.hidden and show_hidden else ""
    chip = f"  [{merged.chip}]" if merged.chip else ""
    return f"• {label}: {merged.value}{extra}{chip}"


def _place_bullet(label: str, places: list[Merged]) -> str:
    """Every competing place on ONE bullet, under ONE label.

    The second country used to have a bullet of its own, and a bullet of its own
    is what let it float free of the word "server": two labelled lines read as
    two facts about two places — the server over here, the player over there —
    and an operator concluded the card had located the human. It had not.
    Sharing the label is what makes both flags visibly answers to the same
    question about the same address."""
    parts = [f"{_md(p.value)}{f' {_tag(p.chip)}' if p.chip else ''}" for p in places]
    return f"- **{_md(label)}:** {_PLACE_JOIN.join(parts)}"


def _plain_place_bullet(label: str, places: list[Merged]) -> str:
    parts = [f"{p.value}{f'  [{p.chip}]' if p.chip else ''}" for p in places]
    return f"• {label}: {_PLACE_JOIN.join(parts)}"


def _players_md(res: IpResult) -> str:
    """Player ids pasted with this address, kept inline and copyable."""
    if not res.players:
        return ""
    return " " + " ".join(f"👤`{_md(p)}`" for p in res.players)


def _skipped_lines(results: list[IpResult]) -> list[str]:
    """Addresses that were never looked up, and why."""
    out: list[str] = []
    for res in results:
        if res.resolved:
            continue
        why = res.note or "; ".join(res.failures[:2]) or "no provider returned data"
        out.append(f"`{_md(res.ip)}` — _{_md(why)}_")
    return out


def _plain(markdown: str) -> str:
    """Strip the card's emphasis markers for the plain-text mirror."""
    return markdown.replace("**", "").replace("_", "")


def _places_for(res: IpResult) -> list[Merged]:
    """The competing places for one address, best-supported first.

    Each entry is still one provider's *whole* answer — country and city taken
    together from a single record. Pasting the country that won a cross-provider
    vote next to some other provider's city is what put "Philippines" beside
    "Singapore, Singapore" on one card, and no reader could make sense of that.
    That invariant is unchanged; what changed is which record leads.

    This used to return the first answer by display priority, and it was the only
    merger in this module that ranked priority ahead of support — _merge_names,
    _merge_asns and _merge_country all sort on -len(providers) first. On a VPN
    block that costs real accuracy: for 217.145.74.157 three sources said
    Philippines while IPinfo alone said Singapore, then Australia an hour later
    for the same address, and because IPinfo is first in _PROVIDERS the card
    headlined the one answer that kept moving. The operator's own VPN client
    said Philippines, so the card looked simply wrong. Support decides now, and
    display priority only breaks a tie.

    At most two are returned, and the runner-up must be a real second opinion
    rather than one provider's outlier — a third country, or a lone dissenter
    against a clear majority, is the noise this line exists to suppress."""
    names: dict[str, str] = {}
    for rec in res.good:
        code = rec.country_code.upper()
        if code and rec.country_name and code not in names:
            names[code] = rec.country_name
    groups: dict[str, dict[str, Any]] = {}
    for rec in sorted(res.good, key=lambda r: r.index):
        code = rec.country_code.upper()
        if not code:
            continue
        group = groups.setdefault(
            code, {"providers": set(), "recs": [], "rec": rec, "seq": len(groups)}
        )
        group["providers"].add(rec.key)
        group["recs"].append(rec)
        # Keep the highest-priority record that actually carries a city: IPinfo
        # returns a code with no country name, so a group led by it would render
        # a bare "🇦🇺 AU" when a later provider had the full "Australia · Sydney".
        held = group["rec"]
        if not (held.city or held.region) and (rec.city or rec.region):
            group["rec"] = rec
    if not groups:
        return []

    ordered = sorted(
        groups.items(),
        key=lambda kv: (
            -len(kv[1]["providers"]),
            min(_PROVIDER_INDEX.get(p, len(_PROVIDERS)) for p in kv[1]["providers"]),
            kv[1]["seq"],  # deterministic: ties keep first-seen order
        ),
    )
    top = len(ordered[0][1]["providers"])
    out: list[Merged] = []
    for code, group in ordered[:2]:  # a third country restores the skim surface
        support = len(group["providers"])
        # One provider geolocating a VPN block to its own guess is not a second
        # country. Same rule _merge_country applies, same reason. A runner-up
        # that TIES the leader is never an outlier, though — when only two
        # sources answered and they disagreed, dropping one would hide a
        # coin-flip behind a display-order tiebreak and show it as settled.
        if out and support < top and (support < 2 or 2 * support < top):
            break
        rec = group["rec"]
        label = names.get(code) or rec.country_name or code
        text = f"{_flag(code)} {label}".strip()
        city = rec.city or rec.region
        shown = city if city and city.lower() != label.lower() else ""  # "Singapore · Singapore" is noise
        if shown:
            text += f" · {shown}"
        # The chip counts what backs the line as WRITTEN, not just its country.
        # Everyone in this group agreed on the country, but a provider that named
        # a different city contradicts half of what is displayed, and counting it
        # would turn one source's guess at a city into "[IPinfo +5]" — the same
        # vocabulary the Operator line below uses to mean six sources returned
        # the same string. A provider that offered no city at all contradicts
        # nothing, so it still counts.
        backers = {
            r.key
            for r in group["recs"]
            if not shown
            or not (r.city or r.region)
            or (r.city or r.region).strip().lower() == shown.strip().lower()
        }
        chip, sources = _chip_for(backers)
        out.append(Merged(value=text, chip=chip, sources=sources, keys=frozenset({code})))
    return out


def _headline(res: IpResult) -> tuple[str, _Kind]:
    """The one line the card exists for: where this player is, or the honest
    admission that the address cannot say."""
    kind = _kind_of(set(res.traits))
    if kind.word == "Datacenter":
        return "🏢 **Datacenter IP — not a home connection, real location UNKNOWN**", kind
    if kind.relay:
        return f"{kind.emoji} **{kind.word} detected — the user's real location is UNKNOWN**", kind
    places = _places_for(res)
    where = places[0].value.split(" · ")[0] if places else "unknown"
    if len(places) > 1:
        # Never state a country flat while the bullet underneath is about to
        # show a second one — the headline is the line most people read alone.
        where += " (disputed)"
    what = "Mobile data" if kind.word == "Mobile" else "Real connection"
    return f"{kind.emoji} **{what} — user is in {where}**", kind


# Deleted: _registered(). It printed a bullet labelled "Registered in" that
# never read whois — it returned IpResult.country, the majority vote across the
# same geolocation providers the line above it already quoted. So a card showed
# "VPN server: 🇸🇬 Singapore" over "Registered in: 🇵🇭 Philippines", an operator
# read it as two facts about two places — the server over here, the player over
# there — and concluded the bot had located the human behind the VPN. Nothing
# here can do that. The real registration for that block is GB (RIPE rir-geo,
# the /20) and VG (the RDAP /24 object), neither country the bullet ever
# printed, so the label did not merely read loosely: it named a fact this
# module never fetched. The genuine second opinion it was groping for is now
# the second entry _places_for() returns, under the same label as the first.


@dataclass(frozen=True)
class _Kind:
    """How one address is classified, and every label that follows from it. One
    place to decide it, so the single-address card and the multi-address blocks
    cannot drift into calling the same address different things."""

    emoji: str
    word: str  # "VPN", "Datacenter", … — the block heading
    label: str  # bullet label for the place: "VPN server", "Location", …
    inline: str  # same, lowercased where it reads as prose: "(vpn exit)"
    colour: str
    relay: bool  # geolocation describes a server, not the user
    # The ↳ sentence, as a template taking {subj}; shown only when sources split.
    dispute: str
    noun: str  # what one address IS here, for {subj} — pluralised with "s"


def _kind_of(traits: set[str]) -> _Kind:
    if traits & {"vpn", "proxy", "tor"}:
        word = "VPN" if "vpn" in traits else ("Tor" if "tor" in traits else "Proxy")
        return _Kind(
            "🔒", word, f"{word} server", f"{word} server", "red", True,
            "both are guesses at {subj} — not where the user is",
            f"{word} exit node",
        )
    if "hosting" in traits:
        return _Kind(
            "🏢", "Datacenter", "Server", "server", "orange", True,
            "both are guesses at {subj} — an address here does not locate a person",
            "datacenter server",
        )
    if "mobile" in traits:
        return _Kind(
            "📱", "Mobile", "Location", "", "blue", False,
            "sources disagree on {subj}", "address",
        )
    return _Kind(
        "🏠", "Real", "Location", "", "green", False,
        "sources disagree on {subj}", "address",
    )


def _place_note(places: list[Merged], kind: _Kind, count: int = 1) -> str:
    """The one sentence saying what two places actually mean.

    An address has ONE location, so two answers mean the sources disagree about
    it — it never means the address travelled from one to the other, which is
    precisely how a reader turns "🇵🇭 / 🇸🇬" into "he opened a Philippine VPN
    into Singapore". That story is what an operator built out of two bullets,
    and it is the reason this line exists. The wording comes from _Kind so the
    single-address card and the multi-address block cannot spell it two ways,
    and so a datacenter is not described in terms of a user who is very likely
    not behind it. The ↳ carries the attachment to the bullet above even if
    Lark collapses the indent.

    ``count`` is how many addresses the bullet above is standing for. The
    single-address branch of build_card also runs when several addresses share
    one operator, and there the sentence would otherwise say "this one exit
    node" directly beneath a list of five of them — trading the misreading this
    line removes for a new one."""
    if len(places) < 2:
        return ""
    subj = f"this one {kind.noun}" if count == 1 else f"these {count} {kind.noun}s"
    return f"  _↳ {kind.dispute.format(subj=subj)}_"


def _ip_block(res: IpResult) -> str:
    """One address as its own small block, for the multi-IP card.

    Everything on one line wrapped mid-sentence in Lark once the operator name
    was appended, so a two-address answer arrived as four ragged lines with no
    visible boundary between them. A heading line plus indented detail survives
    wrapping."""
    traits = set(res.traits)
    kind = _kind_of(traits)
    places = _places_for(res)
    head = f"{kind.emoji} **{kind.word}** · `{_md(res.ip)}`{_players_md(res)}"

    # No chips in a per-address block — there is no room, the footer already
    # carries the source count, and two flags already show the split.
    where = _PLACE_JOIN.join(_md(p.value) for p in places) or "_location unknown_"
    quals = [q for q in (kind.inline, _DISPUTE_SHORT if len(places) > 1 else "") if q]
    if quals:  # a relay's own city, so say whose city it is
        where += f" _({'; '.join(quals)})_"

    # First name only: a per-address block has no room for the aliases the
    # providers also returned for the same operator.
    tail = [res.isp.value.split(" / ")[0]]
    detail = " · ".join(_md(t) for t in tail if t)
    return "\n".join([head, where] + ([detail] if detail else []))


def build_card(results: list[IpResult], *, elapsed: float = 0.0) -> dict[str, Any]:
    """Lark card JSON v2 — ``markdown`` components, so inline ``<text_tag>``
    chips render as real badges.

    Deliberately short: a verdict, the place, and the operator. ASN and prefix
    ride along in the footer for whoever files an abuse report; everything else
    the providers return is detail that pushed the answer off the card."""
    summary = summarise(results)
    resolved = [res for res in results if res.resolved]
    elements: list[dict[str, Any]] = []
    colour = "grey"

    if len(resolved) == 1 or (resolved and len({r.isp.value for r in resolved}) == 1):
        res = resolved[0]
        banner, kind = _headline(res)
        colour, place_label = kind.colour, kind.label
        lines = [banner, ""]
        addrs = ", ".join(f"`{_md(r.ip)}`{_players_md(r)}" for r in resolved)
        lines.append(addrs)
        places = _places_for(res)
        if places:
            lines.append(_place_bullet(place_label, places))
            note = _place_note(places, kind, len(resolved))
            if note:
                lines.append(note)
        if summary.isp:
            lines.append(_bullet("Operator", summary.isp, show_hidden=False))
        elements.append(_text_element("\n".join(lines)))
    elif resolved:
        flagged = [r for r in resolved if set(r.traits) & set(_SUSPECT)]
        if not flagged:
            banner, colour = "🏠 **All real connections**", "green"
        elif len(flagged) == len(resolved):
            banner, colour = (
                f"🔒 **All {len(resolved)} are VPN / datacenter — real locations UNKNOWN**",
                "red",
            )
        else:
            verb = "is" if len(flagged) == 1 else "are"
            banner, colour = (
                f"🔒 **{len(flagged)} of {len(resolved)} {verb} VPN / datacenter — "
                "location unknown for those**",
                "orange",
            )
        blocks = "\n\n".join(_ip_block(r) for r in resolved)
        elements.append(_text_element(f"{banner}\n\n{blocks}"))

    skipped = _skipped_lines(results)
    if skipped:
        if elements:  # nothing resolved — no rule needed above the first block
            elements.append({"tag": "hr"})
        elements.append(_text_element("**Not looked up**\n" + "\n".join(skipped)))

    # Card JSON v2 has no "note" component — its component list is div / markdown
    # / hr / img / … — so the footer is an italic text block instead. A source
    # count, not six provider names: the names filled a whole line and only
    # matter when one is missing, which the count already shows.
    foot = [summary.asn.value] if summary.asn else []
    if summary.prefixes:
        foot.append(" / ".join(summary.prefixes[:2]))
    foot.append(f"🔎 {len(summary.sources)}/{len(_PROVIDERS)} sources")
    if elapsed:
        foot.append(f"{elapsed:.1f}s")
    if elements and not skipped:  # a rule already separates the skipped block
        elements.append({"tag": "hr"})
    elements.append(_text_element(f"_{_md(' · '.join(foot))}_"))

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": colour,
            "title": {"tag": "plain_text", "content": "🌐 IP / ISP lookup"},
        },
        "body": {"elements": elements},
    }


def build_text(results: list[IpResult], *, elapsed: float = 0.0) -> str:
    """Plain-text mirror of the card, for when the interactive send is rejected."""
    summary = summarise(results)
    resolved = [res for res in results if res.resolved]
    lines: list[str] = []

    if len(resolved) == 1 or (resolved and len({r.isp.value for r in resolved}) == 1):
        res = resolved[0]
        banner, kind = _headline(res)
        place_label = kind.label
        lines.append(_plain(banner))
        lines.append("")
        for r in resolved:
            tag = f" (player {', '.join(r.players)})" if r.players else ""
            lines.append(f"{r.ip}{tag}")
        places = _places_for(res)
        if places:
            lines.append(_plain_place_bullet(place_label, places))
            note = _place_note(places, kind, len(resolved))
            if note:
                lines.append(_plain(note))
        if summary.isp:
            lines.append(_plain_bullet("Operator", summary.isp, show_hidden=False))
    elif resolved:
        flagged = [r for r in resolved if set(r.traits) & set(_SUSPECT)]
        if not flagged:
            lines.append("🏠 All real connections")
        elif len(flagged) == len(resolved):
            lines.append(f"🔒 All {len(resolved)} are VPN / datacenter — real locations UNKNOWN")
        else:
            lines.append(
                f"🔒 {len(flagged)} of {len(resolved)} "
                f"{'is' if len(flagged) == 1 else 'are'} VPN / datacenter — "
                "location unknown for those"
            )
        for r in resolved:
            lines.append("")
            lines.extend(_plain(_ip_block(r)).replace("`", "").split("\n"))

    unresolved = [res for res in results if not res.resolved]
    if unresolved:
        if lines:  # nothing resolved — do not open the message with a blank line
            lines.append("")
        lines.append("Not looked up:")
        for res in unresolved:
            why = res.note or "; ".join(res.failures[:2]) or "no provider returned data"
            lines.append(f"• {res.ip} — {why}")

    foot = [summary.asn.value] if summary.asn else []
    if summary.prefixes:
        foot.append(" / ".join(summary.prefixes[:2]))
    foot.append(f"{len(summary.sources)}/{len(_PROVIDERS)} sources")
    if elapsed:
        foot.append(f"{elapsed:.1f}s")
    lines.append("")
    lines.append("🔎 " + " · ".join(foot))
    return "\n".join(lines)


def handle_isp_command(query: str) -> tuple[Optional[dict[str, Any]], str]:
    """``/isp`` entry point. Returns ``(card, text)``; ``card`` is None for usage
    and argument errors, where ``text`` is the message to send as-is."""
    ips, players, bad = parse_ip_players(query)
    if not ips:
        extra = f"\n⚠️ Not an IP address: {', '.join(bad[:5])}" if bad else ""
        return None, USAGE + extra
    limit = _max_ips()
    dropped = ips[limit:]
    ips = ips[:limit]
    started = time.monotonic()
    results = lookup(ips, players)
    elapsed = time.monotonic() - started
    card = build_card(results, elapsed=elapsed)
    text = build_text(results, elapsed=elapsed)
    warnings = []
    # Natural-language routing ("isp lookup 8.8.8.8") hands the prose through as
    # arguments, so once at least one IP parsed, only complain about tokens that
    # were plainly *meant* to be addresses — never about ordinary words. A long
    # run of digits counts: pasting a log line mixes ids in with the addresses,
    # and dropping them without a word makes the card look like it covered
    # everything.
    malformed = [
        t
        for t in bad
        if any(c.isdigit() for c in t)
        and ("." in t or ":" in t or (len(t) >= 5 and t.isdigit()))
    ]
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
