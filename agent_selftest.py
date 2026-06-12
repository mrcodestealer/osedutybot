"""
Self-test + iterate harness for the three agents.

It throws hundreds of *human-style* questions at the router (chathandleagent),
the command agent (commandagent) and the chat agent (chatagent), then reports
where they disagree with the expected behaviour. Use it to "keep trying until
the agents are smart" without manually poking the bot.

Usage (Windows)
---------------
    python agent_selftest.py                 # test current models, print score
    python agent_selftest.py --train         # train both, then test
    python agent_selftest.py --loop 3        # train+test up to 3x, keep best
    python agent_selftest.py --epochs 8      # epochs for --train / --loop
    python agent_selftest.py --show-pass      # also print the cases that passed

Exit code is 0 only when every case passes (handy for CI / a retrain loop).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# Each case: (text, expected_route, expected_command_prefix_or_None)
#   expected_route ∈ {"command", "chat"}
#   expected_command_prefix: when set, the mapped command must start with it.
CASES: list[tuple[str, str, str | None]] = [
    # -- prod-batch maintenance (the reported /nwrsetmaintenance bug) --------
    ("i want nwr set maintenance and provide those machines: NWR2113, NWR2114", "command", "/nwrsetmaintenance"),
    ("nwr set maintenance NWR2113 NWR2114", "command", "/nwrsetmaintenance"),
    ("please set maintenance for nwr machines 2113, 2114", "command", "/nwrsetmaintenance"),
    ("put nch in maintenance NCH1422", "command", "/nchsetmaintenance"),
    ("set winford to maintenance, machines: WF8092", "command", "/wfsetmaintenance"),
    ("unset maintenance for nwr NWR2113", "command", "/nwrunsetmaintenance"),
    ("turn off maintenance on nch NCH1422", "command", "/nchunsetmaintenance"),
    ("set nwr to test NWR2113", "command", "/nwrsettest"),
    ("set both maintenance and test for nwr NWR2113", "command", "/nwrsetmaintenancetest"),
    ("can you set maintenance for nwr 2113", "command", "/nwrsetmaintenance"),
    ("i want to set maintenance", "command", None),  # site missing -> still a command attempt
    # -- duty lookups --------------------------------------------------------
    ("who is on fpms duty today", "command", "/fpms"),
    ("show me bi duty", "command", "/bi"),
    ("sre on call now", "command", "/sre"),
    ("who covers cpms today", "command", "/cpms"),
    ("i want fpms today", "command", "/fpms"),
    ("database duty today", "command", "/db"),
    ("ote roster today", "command", "/ote"),
    ("current ose duty", "command", "/ose"),
    # -- leave / wfh / holiday ----------------------------------------------
    ("who is on leave today", "command", "/wholeave"),
    ("show fpms leave this month", "command", "/leave"),
    ("who is wfh in bi", "command", "/wfh"),
    ("upcoming holidays", "command", "/holiday"),
    ("any holiday this month", "command", "/holidaythismonth"),
    # -- machines ------------------------------------------------------------
    ("lookup nwr 2005", "command", "/nwr"),
    ("show nch 1422", "command", "/nch"),
    ("machine winford 8092", "command", "/wf"),
    ("what is nwr 2140", "command", "/nwr"),
    ("check credit NCH1422", "command", "/checkcredit"),
    # -- search people -------------------------------------------------------
    ("who is David", "command", "/s"),
    ("find Henry in duty list", "command", "/s"),
    ("phone number for Ryan", "command", "/s"),
    # -- helpers -------------------------------------------------------------
    ("help", "command", "/help"),
    ("what can you do", "command", None),
    ("what is today", "command", "/date"),
    # -- pure chat (must NOT become a command) ------------------------------
    ("hi", "chat", None),
    ("hello there", "chat", None),
    ("good morning", "chat", None),
    ("how are you", "chat", None),
    ("how are you doing today", "chat", None),
    ("thanks a lot", "chat", None),
    ("thank you so much", "chat", None),
    ("bye", "chat", None),
    ("good night", "chat", None),
    ("lol that's funny", "chat", None),
    ("you're awesome", "chat", None),
    ("i'm so bored", "chat", None),
    ("i'm tired today", "chat", None),
    ("who are you", "chat", None),
    ("happy friday", "chat", None),
    ("nice job bot", "chat", None),
    ("just saying hi", "chat", None),
    ("coffee time", "chat", None),
]


def _route(text: str):
    import chathandleagent

    return chathandleagent.route(text)


def run_tests(*, show_pass: bool = False) -> tuple[int, int]:
    passed = 0
    failed = 0
    for text, exp_route, exp_cmd in CASES:
        d = _route(text)
        ok_route = d.kind == exp_route
        ok_cmd = True
        if exp_cmd is not None:
            cmd = (d.command or "")
            ok_cmd = cmd.lstrip().lower().startswith(exp_cmd.lower())
        ok = ok_route and ok_cmd
        if ok:
            passed += 1
            if show_pass:
                print(f"  PASS  [{d.kind:7}] {text!r} -> {(d.command or '').splitlines()[0] if d.command else ''}")
        else:
            failed += 1
            detail = f"got route={d.kind} (reason={d.reason}) cmd={d.command!r}"
            want = f"want route={exp_route}" + (f" cmd~={exp_cmd!r}" if exp_cmd else "")
            print(f"  FAIL  {text!r}\n        {want}\n        {detail}")
    total = passed + failed
    acc = passed / total if total else 0.0
    print(f"\n{'='*60}\nResult: {passed}/{total} passed  ({acc:.1%})\n{'='*60}")
    return passed, total


def train_all(epochs: int) -> None:
    print(f"\n>>> Training commandagent ({epochs} epochs)…")
    import commandagent

    commandagent.train_model(commandagent.DEFAULT_MODEL_DIR, epochs=epochs)
    print(f"\n>>> Training chatagent classifier ({epochs} epochs)…")
    import chatagent

    chatagent.train_model(chatagent.DEFAULT_MODEL_DIR, epochs=epochs)
    # Reset cached singletons so fresh models are used.
    commandagent._classifier_singleton = None
    commandagent._classifier_failed = False
    chatagent._classifier_singleton = None
    chatagent._classifier_failed = False


def main() -> None:
    ap = argparse.ArgumentParser(description="Self-test + iterate the Duty Bot agents")
    ap.add_argument("--train", action="store_true", help="train both agents before testing")
    ap.add_argument("--loop", type=int, default=0, help="retrain+test up to N times, stop at 100%%")
    ap.add_argument("--epochs", type=int, default=8)
    ap.add_argument("--show-pass", action="store_true")
    args = ap.parse_args()

    # Routing needs AI on; force it for this process so the model is consulted.
    import os

    os.environ.setdefault("BOT_USE_AI", "1")
    os.environ.setdefault("BOT_USE_CHATAGENT", "1")
    os.environ.setdefault("BOT_USE_CHATHANDLE", "1")

    if args.loop and args.loop > 0:
        best = -1
        for i in range(1, args.loop + 1):
            print(f"\n########## Iteration {i}/{args.loop} ##########")
            train_all(args.epochs)
            passed, total = run_tests(show_pass=args.show_pass)
            best = max(best, passed)
            if passed == total:
                print("All cases pass — stopping early. 🎉")
                break
        sys.exit(0 if best == len(CASES) else 1)

    if args.train:
        train_all(args.epochs)

    passed, total = run_tests(show_pass=args.show_pass)
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
