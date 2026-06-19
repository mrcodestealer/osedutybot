"""One-off diagnostic: run on the SERVER to see why offset cell notes are/aren't added.

Usage:
    python offset_note_probe.py            # dump state + run re-ensure + test a safe cell
    python offset_note_probe.py 10 5       # also POST a probe note at row=10 col=5 (0-based)

Delete this file after debugging.
"""
import json
import sys

import requests

import ose_Duty as od


def _dump_state() -> None:
    print("=== offset_shift_sheet_applied.json ===")
    try:
        with open(od._OFFSET_SHIFT_SHEET_APPLIED_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
        print(json.dumps(data, ensure_ascii=False, indent=2)[:4000])
    except FileNotFoundError:
        print("(no state file yet — nothing has been applied on this host)")
    except Exception as exc:
        print(f"(could not read state: {exc!r})")


def _probe_cell(token: str, row_idx: int, col_idx: int) -> None:
    base = f"https://open.larksuite.com/open-apis/drive/v1/files/{od.SPREADSHEET_TOKEN}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    print(f"\n=== POST new_comments  r{row_idx}c{col_idx} ===")
    body_v2 = {
        "file_type": "sheet",
        "reply_elements": [{"type": "text", "text": "PROBE note v2"}],
        "anchor": {"block_id": od.SHEET_ID, "sheet_col": col_idx, "sheet_row": row_idx},
    }
    r = requests.post(f"{base}/new_comments", headers=headers, params={"file_type": "sheet"}, json=body_v2, timeout=60)
    print("HTTP", r.status_code, "->", json.dumps(r.json(), ensure_ascii=False))

    print(f"\n=== POST comments (v1)  r{row_idx}c{col_idx} ===")
    body_v1 = {
        "anchor": {"sheet_id": od.SHEET_ID, "sheet_col": col_idx, "sheet_row": row_idx},
        "reply_list": {"replies": [{"content": {"elements": [{"type": "text_run", "text_run": {"text": "PROBE note v1"}}]}}]},
    }
    r = requests.post(f"{base}/comments", headers=headers, params={"file_type": "sheet"}, json=body_v1, timeout=60)
    print("HTTP", r.status_code, "->", json.dumps(r.json(), ensure_ascii=False))


def main() -> None:
    print("SPREADSHEET_TOKEN:", od.SPREADSHEET_TOKEN)
    print("SHEET_ID:", od.SHEET_ID)
    token = od.get_tenant_access_token()
    print("token ok:", bool(token))

    _dump_state()

    print("\n=== running reensure_applied_offset_shift_sheet_styles_and_notes() ===")
    try:
        stats = od.reensure_applied_offset_shift_sheet_styles_and_notes()
        print("re-ensure stats:", stats)
    except Exception as exc:
        print("re-ensure raised:", repr(exc))

    if len(sys.argv) >= 3:
        _probe_cell(token, int(sys.argv[1]), int(sys.argv[2]))


if __name__ == "__main__":
    main()
