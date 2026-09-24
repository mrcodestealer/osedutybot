#!/usr/bin/env python3
"""Who owns a notice - vawatch._attribute, end to end through handle_messages.

  python3 testing/vawatch_attribution_test.py [-v]      exit 1 on any failure

OFFLINE. Every network, Base and send function is replaced by a fake before
anything runs, and a real network call raises - nothing can reach Lark or
Telegram. The live 22-row watch list is loaded so the "names another
provider" rules see the same names production does.

Each case is a trigger from the 2026-09-24 audit (R1.56 no-impact / partial
notices, R1.60 the operator's own notice or one written TO the provider,
R1.64 multi-provider schedules, R1.66 other brands and forwards, R1.78 real
notices the ownership check used to refuse) plus notices that must still fill.
"""
import datetime as d, pathlib, sys, tempfile
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import vawatch as va
NET = {"n": 0}
def _blocked(*a, **k):
    NET["n"] += 1
    raise RuntimeError("network blocked in test")
va._tenant_token = _blocked

# Confirm-before-write: the model is STUBBED to agree, so these tests keep
# testing the rules; the gate itself is tested in testing/vawatch_state_test.py
# and testing/providerllm_confirm_test.py. Never a real model call.
CONFIRM = {"verdict": "yes", "transient": False, "why": "test stub: confirmed"}
va._llm_confirm = lambda *a, **k: dict(CONFIRM)
# audit-7: the clock is PINNED. This file used to run on the real wall clock
# against fixed 2026-09-25 / 2026-10-2x windows, so it began failing the day
# after 2026-09-25 12:00 +08, and before that a 'nowrite' case could pass only
# because its window had gone stale. va.classify reads va._now_dt, so this pins
# the parser's `stale` too.
TZ8 = d.timezone(d.timedelta(hours=8))
NOW = d.datetime(2026, 9, 22, 12, 0, tzinfo=TZ8)
va._now_dt = lambda: NOW
if hasattr(va, "requests"):
    import types
    va.requests = types.SimpleNamespace(get=_blocked, post=_blocked, put=_blocked, request=_blocked)
WR, CARDS = [], []
va.find_provider_row = lambda p: {"record_id": "rec_" + p, "fields": {}}
va.update_row = lambda rid, f: WR.append((rid, f)) or {"code": 0}
va.send_card = lambda chat, card: CARDS.append(card) or {"code": 0}
va.send_text = lambda chat, t: CARDS.append(t) or {"code": 0}
LIVE = [("5G","CP x 5G Integration_new"),("BNG","BNG x Casinoplus Integratioin Group"),("CQ9","CQ9-IGO 客服群"),
 ("EEZE Slot","CP x Eeze Slot General"),("FC","FC - CasinoPlus 技術服務群"),("Hacksaw","[SG190- IGO Casinoplus YG/ RG/ HS] CS group"),
 ("JDB","JDB & IGO (IGOS 單) 合規 技術群 Casino Plus"),("JILI","IGO & JL_seamless (legal)"),("KingMidas","[R] KM x IGO / CasinoPlus"),
 ("OMNIPLAY","CasinoPlus x OMNIPLAY x Integration Group"),("PP","PP - IGO PR [A-SW-S/LC][A-SPE14-2117]"),
 ("Playstar","[EIGO] IGO x PLAYSTAR API"),("SimplePlay","SM781 - SimplePlay - IGO Digital HighTechnology Inc."),
 ("VA","🆕VA 公告｜VA announcements"),("VP","VP x casino plus(CP單) Integration Group"),
 ("YGG","[SG190- IGO Casinoplus YG/ RG/ HS] CS group"),("YGR","CasinoPlus - YGR API (seamless)"),("Yellow Bat","(OG) IGO / YB")]
REP = {"telegram": [{"provider": p, "group": g, "record_id": "rec_" + p} for p, g in LIVE],
       "teams": [{"provider": p, "group": p} for p in ("GEMINI", "PG Soft", "RTG")],
       "skipped": [{"provider": "Sport/Ebet", "group": "[CasinoPlus] CasinoPlus x BTi(SL) Support"}]}
GROUP = dict(LIVE)
C = []
def c(key, provider, text, want, start=None):
    C.append((key, provider, text, want, start))

def run_one(provider, text):
    va.LEDGER_PATH = pathlib.Path(tempfile.mkdtemp()) / "v.json"
    if hasattr(va, "_LAST_WATCH"):
        va._LAST_WATCH["rep"] = REP
    g = GROUP[provider]
    shared = [p for p, gg in LIVE if gg == g and p != provider]
    va.handle_messages([{"mid": "100", "text": "hello team"}], provider=provider, group=g, shared_with=shared)
    WR.clear(); CARDS.clear()
    r = va.handle_messages([{"mid": "101", "text": text}], provider=provider, group=g, shared_with=shared)
    return list(WR), list(CARDS), r

def main(verbose=False):
    bad = 0
    for key, prov, text, want, start in C:
        wr, cards, r = run_one(prov, text)
        wrote = [f for rid, f in wr if f.get("Start Time")]
        got_start = None
        if wrote:
            got_start = d.datetime.fromtimestamp(wrote[-1]["Start Time"] / 1000, d.timezone(d.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
        if want == "write":
            ok = bool(wrote) and (start is None or got_start == start)
        elif want == "nowrite":
            ok = not wrote
        else:  # "nowrite-or:<start>"
            ok = (not wrote) or got_start == want.split(":", 1)[1]
        why = (r.get("details") or [{}])[-1]
        if not ok: bad += 1
        if verbose or not ok:
            print(f"{'ok  ' if ok else 'FAIL'} {key:9} {prov:10} wrote={got_start or '-':16} "
                  f"{str(why.get('action',''))[:16]:16} | {text[:66]!r}  [{str(why.get('why') or why.get('reason') or '')[:60]}]")
    print(f"{len(C) - bad}/{len(C)} pass   | real network attempts: {NET['n']}")
    return bad

# R1.56 the provider's own no-impact / partial-outage notice
for t in ["后台系统维护 2026-09-25 10:00-12:00，游戏不受影响。",
          "Scheduled maintenance for BO 2026-09-25 10:00-12:00, gameplay unaffected",
          "Scheduled maintenance 2026-09-25 10:00-12:00. Game service will remain available.",
          "Scheduled maintenance 2026-09-25 10:00-12:00; players can continue playing as usual.",
          "Backoffice will be under maintenance 2026-09-25 10:00-12:00, games are running normally.",
          "Scheduled maintenance 2026-09-25 10:00-12:00, there will be no impact on gameplay.",
          "Scheduled maintenance 2026-09-25 10:00-12:00, services won't be interrupted.",
          "Scheduled maintenance 2026-09-25 10:00-12:00 (EU region only). Asia operators are not affected.",
          "Scheduled maintenance for Sweet Bonanza only, 2026-09-25 10:00-12:00; all other games available.",
          "Scheduled maintenance of the Live Casino lobby 2026-09-25 10:00-12:00. Slots remain available."]:
    c("R1.56", "PP", t, "nowrite")
# R1.60 the operator's own notice, or one written TO the provider
for t in ["CasinoPlus DB maintenance 2026-09-25 10:00-12:00",
          "Casinoplus database maintenance on 2026-09-25 10:00-12:00 GMT+8",
          "Casino Plus app maintenance 2026-09-25 10:00-12:00",
          "Maintenance on CasinoPlus side: 2026-09-25 10:00-12:00",
          "Notice from CasinoPlus: scheduled maintenance 2026-09-25 10:00-12:00",
          "Casinoplus: scheduled maintenance 2026-09-25 10:00-12:00",
          "【CasinoPlus】系统维护 2026-09-25 10:00-12:00",
          "Operator side (CasinoPlus) will do DB maintenance 2026-09-25 10:00-12:00",
          "Dear Pragmatic Play, our site will be under maintenance 2026-09-25 10:00-12:00",
          "Dear PP team, we will have maintenance 2026-09-25 10:00-12:00",
          "Hi Pragmatic team, FYI we have scheduled maintenance 2026-09-25 10:00-12:00 GMT+8"]:
    c("R1.60", "PP", t, "nowrite")
# R1.64 a multi-provider schedule in a single-provider group
c("R1.64", "PP", "Maintenance schedule this week:\nPragmatic Play: 2026-09-26 02:00-04:00\nEvolution: 2026-09-25 10:00-12:00", "nowrite-or:2026-09-26 02:00")
# R1.66 third party / another brand
for t in ["PAGCOR system maintenance 2026-09-25 10:00-12:00", "Telegram will be down for maintenance 2026-09-25 10:00-12:00",
          "NetEnt maintenance on 2026-09-25 10:00-12:00",
          "Forwarded message: Scheduled maintenance 2026-09-25 10:00-12:00 - Relax Gaming",
          "Scheduled maintenance 2026-09-25 10:00-12:00 (GMT+8)\n\nRegards,\nRelax Gaming Team"]:
    c("R1.66", "PP", t, "nowrite")
# R1.78 real provider notices that must NOT be refused
BODY = "Scheduled maintenance\nDate: 2026-10-14\nTime: 10:00 - 12:00 (GMT+8)\n"
for x in ["Casinoplus side will be unable to launch our games during this period.",
          "CP side will not be able to open the games.", "IGO platform will show the maintenance page.",
          "Your games will show a maintenance message during this time.", "维护期间，贵司的游戏将无法访问。",
          "The maintenance is for our AWS server migration.", "We are migrating our CDN to Cloudflare.",
          "Seamless wallet and e-wallet transfers will be unavailable.",
          "Our payment gateway integration will also be updated.",
          "We will upgrade our internal banking system module.",
          "Players on 4G/5G mobile networks will also be affected."]:
    c("R1.78", "JDB", BODY + x, "write", "2026-10-14 10:00")
# ambiguous speaker: carded, never written (see _OPERATOR_AUDIENCE_RE)
for x in ["Please avoid any releases on your side during the maintenance.",
          "There will be no traffic to the games during this time."]:
    c("R1.78-card", "JDB", BODY + x, "nowrite")
# (The clock is pinned to NOW above, so no window here goes stale with time; a
# 'nowrite' case can no longer pass because its window has ended.)
# G4.1 the operator naming itself as the subject in shapes the subject regex did
# not know: "we (Casinoplus)", "maintenance of/on the Casinoplus side", "the
# Casinoplus site is going under / is undergoing maintenance".
for t in ["Dear partners, we (Casinoplus) will have scheduled maintenance on 2026-10-25 03:00-05:00 (GMT+8).",
          "Please be informed there will be a scheduled maintenance of Casinoplus on 2026-10-25 03:00-05:00 (GMT+8).",
          "Scheduled maintenance on the Casinoplus side 2026-10-25 03:00-05:00 (GMT+8).",
          "Hi all, the Casinoplus site is going under scheduled maintenance on 2026-10-25 03:00-05:00 (GMT+8).",
          "Casinoplus is undergoing maintenance 2026-10-25 03:00-05:00 (GMT+8)."]:
    c("G4.1", "PP", t, "nowrite")
# ...while the operator as the party AFFECTED by the provider's window still writes (R1.63).
c("G4.1-keep", "PP", "Scheduled maintenance 2026-10-25 03:00-05:00 (GMT+8). Casinoplus side will be unable to launch our games during this period.", "write", "2026-10-25 03:00")
# G4.2 banks and gateways the third-party list did not name, plus "<Name> Bank" / "Bank of X".
for t in ["Security Bank scheduled maintenance 2026-10-24 00:00-04:00 (GMT+8).",
          "China Bank will have scheduled maintenance on 2026-10-24 00:00-04:00 (GMT+8).",
          "EastWest Bank scheduled maintenance 2026-10-24 00:00-04:00 (GMT+8).",
          "PNB scheduled maintenance 2026-10-24 00:00-04:00 (GMT+8), online transfers unavailable.",
          "DragonPay scheduled maintenance 2026-10-24 00:00-04:00 (GMT+8).",
          "Bank of the Philippine Islands system maintenance 2026-10-24 00:00-04:00 (GMT+8)."]:
    c("G4.2", "PP", t, "nowrite")
c("G4.2-keep", "PP", "Scheduled maintenance on 2026-10-24 00:00-04:00 (GMT+8). Thank You Bank on us.", "write", "2026-10-24 00:00")
# G4.4 the provider's own no-impact notice in the ACTIVE voice: "does not affect".
for t in ["Please be informed, Evolution's scheduled maintenance on 2026-10-24 10:00-12:00 (GMT+8) does not affect Pragmatic Play.",
          "JILI maintenance on 2026-10-24 10:00-12:00 (GMT+8) won't affect Pragmatic Play games.",
          "Evolution scheduled maintenance 2026-10-24 10:00-12:00 (GMT+8). No impact on Pragmatic Play."]:
    c("G4.4", "PP", t, "nowrite")
# keeps
c("keep", "PP", "Scheduled maintenance 2026-09-25 10:00-12:00 (GMT+8). All Pragmatic Play games will be unavailable.", "write", "2026-09-25 10:00")
c("keep", "KingMidas", '🚧🚧🚧  維護公告 Maintenance Notification  🚧🚧🚧 \n\n尊敬的客户,\nHi Team,\n\n我们恭敬地向您报告， KingMidas - KM系统 将于9/23 (三) 10:00AM (GMT+8) 排定维护，在这个期间 KingMidas - KM系统所有的游戏服务都会暂停使用。\n有任何更新信息，我们将尽快向您报告，不便之处敬请原谅。\n\nPlease note that the KingMidas - Sysyem scheduled maintenance will commence at 10:00AM 23th(Wed) Sep (GMT+8).\nDuring this period, you will not be able to access to all the games.\nWe will keep you posted on the status. Thank you for your understanding of any inconvenience caused.\n\n日期Date：Sep 23th, 2026 (Wed)\n时间Time：10:00 - 12:00  (GMT+8)\n受影响之游戏Affected Game :\n正式环境所有的游戏服务、后台与API\nAll games(Slots, TableGames), Backoffice and API service on Production\n'.replace("9/23", "10/23").replace("Sep 23th", "Oct 23th").replace("23th(Wed) Sep", "23th(Fri) Oct").replace("(Wed)", "(Fri)").replace("(三)", "(五)"), "write", "2026-10-23 10:00")

# G4.1 (#93-#98, #279): the operator naming itself in shapes the subject rule
# missed - a greeting then a comma, the hyphenated name, a market code before
# the verb, two nouns, "by"/"at" the operator, a self-introduction, a dash or
# bar heading, 我方（X）, and a salutation to the row's provider.
for t in ["Hello, CP platform maintenance 2026-09-25 03:00-05:00 (GMT+8), please take note.",
          "Casino-Plus scheduled maintenance 25/09/2026 10:00-12:00 GMT+8",
          "CasinoPlus PH will undergo scheduled maintenance on 25/09/2026 10:00-12:00 (GMT+8).",
          "The CasinoPlus website and app will be under maintenance on 25/09/2026 10:00-12:00 (GMT+8).",
          "Scheduled maintenance by CasinoPlus 25/09/2026 10:00-12:00 (GMT+8).",
          "Hi CP team here, scheduled maintenance 25/09/2026 10:00-12:00 (GMT+8) on our platform.",
          "CasinoPlus - Scheduled maintenance 2026-09-25 10:00-12:00",
          "CasinoPlus | scheduled maintenance 2026-09-25 10:00-12:00",
          "Maintenance at CasinoPlus 2026-09-25 10:00-12:00",
          "我方（CasinoPlus）系统维护 2026-09-25 10:00-12:00",
          "Dear partner PP, we will have scheduled maintenance on 2026-09-25 10:00-12:00"]:
    c("G4.1b", "PP", t, "nowrite")
# G4.1 (#99-#102): addressed to providers in general, so the operator speaks.
for t in ["Dear provider, kindly be informed that we will be performing scheduled maintenance on 25/09/2026 10:00-12:00 (GMT+8).",
          "To all providers: scheduled maintenance on our end 25/09/2026 10:00-12:00 (GMT+8).",
          "Dear game providers, please be informed of our scheduled maintenance on 25/09/2026 10:00-12:00 (GMT+8).",
          "致各游戏供应商：我方将于2026年9月25日10:00-12:00 (GMT+8) 进行系统维护。"]:
    c("G4.1c", "PP", t, "nowrite")
# ...while a provider's "Dear partners," greeting still writes.
c("G4.1c-keep", "PP", "Dear partners, scheduled maintenance on 25/09/2026 10:00-12:00 (GMT+8). All Pragmatic Play games will be unavailable.", "write", "2026-09-25 10:00")
# G4.2 (#103-#112): telcos, remittance, interbank, crypto rails, 支付宝, 阿里云.
for t in ["Our payment partner will have scheduled maintenance on 2026-09-24 00:00-04:00 (GMT+8).",
          "Sun Cellular network maintenance 2026-09-24 00:00-04:00 (GMT+8).",
          "Smart scheduled network maintenance 24/09/2026 00:00-02:00 (GMT+8).",
          "TNT network maintenance 24/09/2026 00:00-02:00 (GMT+8), mobile data may be slow.",
          "Cebuana Lhuillier scheduled maintenance 24/09/2026 00:00-02:00 (GMT+8).",
          "Bancnet scheduled maintenance 24/09/2026 00:00-02:00 (GMT+8), ATM transactions unavailable.",
          "PSP scheduled maintenance 24/09/2026 00:00-02:00 GMT+8, deposits may fail.",
          "TRON network scheduled maintenance 24/09/2026 00:00-02:00 (GMT+8), USDT deposits delayed.",
          "Binance scheduled maintenance 24/09/2026 00:00-02:00 (GMT+8).",
          "支付宝将于2026年9月24日00:00-02:00进行系统维护，届时充值不可用。",
          "阿里云将于2026年9月24日00:00-02:00 (GMT+8) 进行计划维护。"]:
    c("G4.2b", "PP", t, "nowrite")
# ...and the provider's own outage that lists USDT deposits as affected still writes.
c("G4.2b-keep", "PP", "Scheduled maintenance on 2026-09-24 00:00-04:00 (GMT+8). USDT deposits will be unavailable.", "write", "2026-09-24 00:00")
# G4.4 (#118): the speaker says the window is not the addressee's.
for t in ["Please note there is emergency maintenance on 24/09/2026 14:00-16:00 GMT+8, not your games.",
          "FYI scheduled maintenance on 24/09/2026 14:00-16:00 GMT+8 - this does not concern your games.",
          "FYI another provider has scheduled maintenance on 24/09/2026 14:00-16:00 GMT+8."]:
    c("G4.4b", "PP", t, "nowrite")
# #283: a multi-provider schedule written with dashes / spaces, not colons.
c("#283", "PP", "Maintenance schedule:\nPP - 2026-09-26 02:00-04:00\nJILI - 2026-09-25 10:00-12:00", "nowrite-or:2026-09-26 02:00")
c("#283", "PP", "Maintenance schedule:\nPragmatic 2026-09-26 02:00-04:00\nNetEnt 2026-09-25 10:00-12:00", "nowrite-or:2026-09-26 02:00")
# #284: a test / template / example message.
for t in ["test test Scheduled maintenance 2026-09-26 10:00-12:00",
          "#test Scheduled maintenance 2026-09-26 10:00-12:00",
          "Template - Scheduled maintenance 2026-09-26 10:00-12:00",
          "For example, Scheduled maintenance 2026-09-26 10:00-12:00",
          "Mock notice: Scheduled maintenance 2026-09-26 10:00-12:00",
          "Dummy: Scheduled maintenance 2026-09-26 10:00-12:00",
          "Format: Scheduled maintenance 2026-09-26 10:00-12:00"]:
    c("#284", "PP", t, "nowrite")
# audit-1: a notice that says the row's provider is LEFT OUT of the maintenance.
for t in ["Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8). Pragmatic Play games are excluded from this maintenance.",
          "Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8) (not including PP).",
          "Maintenance on 2026-09-25 10:00-12:00 (GMT+8) without PP games."]:
    c("audit-1", "PP", t, "nowrite")
for t in ["Maintenance on 2026-09-25 10:00-12:00 (GMT+8) - games from Hacksaw excluded.",
          "Maintenance on 2026-09-25 10:00-12:00 (GMT+8) without Hacksaw games.",
          "Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8). Hacksaw games are excluded from this maintenance.",
          "Scheduled maintenance on 2026-09-25 10:00-12:00 (GMT+8) (not including Hacksaw)."]:
    c("audit-1", "Hacksaw", t, "nowrite")
# ...while "without prior notice" on the row's own notice is not a negation.
c("audit-1-keep", "Hacksaw", "Hacksaw emergency maintenance on 2026-09-25 10:00-12:00 (GMT+8), without prior notice.", "write", "2026-09-25 10:00")
# #296: the 5G network, not the provider 5G, in a JDB notice.
for x in ["Players on 5G networks may experience brief disconnects.",
          "5G and 4G users are also affected.",
          "All players including 5G users will be affected."]:
    c("#296", "JDB", BODY + x, "write", "2026-10-14 10:00")

if __name__ == "__main__":
    sys.exit(1 if main("-v" in sys.argv) else 0)
