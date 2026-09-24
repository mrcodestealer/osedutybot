#!/usr/bin/env python3
"""Measure the confirm-before-write gate against the REAL local model.

  python3 testing/confirm_gate_eval.py              all cases
  python3 testing/confirm_gate_eval.py --limit 20   a quick look
  python3 testing/confirm_gate_eval.py --show       print every disagreement

Run it on the bot's server BEFORE turning VAWATCH_ENABLED back on. It sends
nothing to Lark or Telegram and writes nothing to any Base: each case is parsed
by noticeparse and, when the parser would write, put to providerllm's
confirm_window - the one network call, to BOT_CHAT_API_BASE, which the gate
itself refuses unless it is local (see providerllm.confirm_endpoint_problem).

Cases (testing/confirm_eval_cases.json):
  confirm  a notice the parser reads correctly - the model should say yes
  reject   a message the parser reads as a fill but that must NOT be written
           (the 2026-09-24 adversarial hunt) - the model should say no

Reported: of the reject cases the parser fills, how many the model stops
(that is the gate's whole point), and of the confirm cases, how many it wrongly
sends to a person (the price: a card instead of an automatic write).
"""
import argparse
import datetime as d
import json
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import noticeparse as np  # noqa: E402
import providerllm as pl  # noqa: E402

try:
    import vawatch as va  # noqa: E402 - only for provider aliases
    names_for = va._names_for
except Exception:  # noqa: BLE001
    def names_for(p):
        return [p]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    problem = pl.confirm_endpoint_problem()
    if problem:
        print(f"refusing to run: {problem}")
        return 2
    print(f"model {pl.model()} at {pl._base_url()}  (warming up...)", flush=True)
    pl.warmup()
    cases = json.load(open(ROOT / "testing" / "confirm_eval_cases.json", encoding="utf-8"))["cases"]
    if args.limit:
        cases = cases[:args.limit // 2] + [c for c in cases if c["label"] == "reject"][:args.limit // 2]
    tally = {"confirm": {"yes": 0, "no": 0, "unclear": 0, "not_fill": 0},
             "reject": {"yes": 0, "no": 0, "unclear": 0, "not_fill": 0}}
    slow = []
    for i, c in enumerate(cases, 1):
        now = d.datetime.fromisoformat(c["now"].replace(" ", "T", 1)) if "T" in c["now"] or " " in c["now"] else None
        v = np.classify(c["text"], now=now)
        if v["action"] != "fill":
            tally[c["label"]]["not_fill"] += 1
            continue
        t0 = time.time()
        g = pl.confirm_window(c["text"], provider=c["provider"], start=v["start"], end=v["end"],
                              names=list(names_for(c["provider"]) or []),
                              reschedule=bool(v.get("reschedule")))
        slow.append(time.time() - t0)
        key = g["verdict"] if g["verdict"] in ("yes", "no") else "unclear"
        tally[c["label"]][key] += 1
        wrong = (c["label"] == "confirm" and key != "yes") or (c["label"] == "reject" and key == "yes")
        if args.show and wrong:
            print(f"  [{c['label']}] model={key} {g.get('why', '')[:90]}\n      {c['text'][:140]!r}")
        if i % 20 == 0:
            print(f"  {i}/{len(cases)}", flush=True)
    r, k = tally["reject"], tally["confirm"]
    r_fill = r["yes"] + r["no"] + r["unclear"]
    k_fill = k["yes"] + k["no"] + k["unclear"]
    print("-" * 78)
    print(f"reject cases the parser would WRITE: {r_fill}")
    print(f"  stopped by the model: {r['no'] + r['unclear']}  ({(r['no'] + r['unclear']) / max(1, r_fill):.0%})"
          f"   still written: {r['yes']}")
    print(f"confirm cases the parser would write: {k_fill}")
    print(f"  confirmed: {k['yes']}   sent to a person instead: {k['no'] + k['unclear']}"
          f"  ({(k['no'] + k['unclear']) / max(1, k_fill):.0%})")
    if slow:
        slow.sort()
        print(f"model time per write: median {slow[len(slow) // 2]:.1f}s, max {slow[-1]:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
