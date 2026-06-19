"""Find the working Lark sheet-cell-comment request shape, on the SERVER.

Usage:
    python offset_note_probe.py <row> <col>      # 0-based matrix indices of a cell

Pick a cell you don't mind annotating (e.g. one of the failing offset cells from the
log, like row=34 col=172). It tries several request variants and prints code/msg for
each so we can see which one the API accepts. Delete this file after debugging.
"""
import json
import sys

import requests

import ose_Duty as od


def _try(name, method, url, params, body, headers):
    try:
        resp = requests.request(method, url, headers=headers, params=params, json=body, timeout=60)
        data = resp.json()
    except Exception as exc:
        print(f"[{name}] EXCEPTION {exc!r}")
        return None
    code = data.get("code")
    ok = "  <<< OK" if code == 0 else ""
    print(f"[{name}] HTTP {resp.status_code} code={code} msg={data.get('msg')!r}{ok}")
    if code == 0:
        print(f"        data={json.dumps(data.get('data'), ensure_ascii=False)}")
    return data


def main():
    if len(sys.argv) < 3:
        print("usage: python offset_note_probe.py <row> <col>   (0-based)")
        return
    row = int(sys.argv[1])
    col = int(sys.argv[2])
    tok = od.SPREADSHEET_TOKEN
    sid = od.SHEET_ID
    text = "PROBE offset note"
    token = od.get_tenant_access_token()
    print(f"token ok={bool(token)} spreadsheet={tok} sheet={sid} cell row={row} col={col}\n")

    base = f"https://open.larksuite.com/open-apis/drive/v1/files/{tok}"
    H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    a1 = f"{od.col_index_to_letter(col + 1)}{row + 1}"

    re_elems = [{"type": "text", "text": text}]
    reply_list = {"replies": [{"content": {"elements": [{"type": "text_run", "text_run": {"text": text}}]}}]}

    # (name, method, url, params, body)
    variants = [
        ("v1 new_comments q+body block_id",
         "POST", f"{base}/new_comments", {"file_type": "sheet"},
         {"file_type": "sheet", "reply_elements": re_elems,
          "anchor": {"block_id": sid, "sheet_col": col, "sheet_row": row}}),

        ("v2 new_comments body-only block_id",
         "POST", f"{base}/new_comments", None,
         {"file_type": "sheet", "reply_elements": re_elems,
          "anchor": {"block_id": sid, "sheet_col": col, "sheet_row": row}}),

        ("v3 new_comments sheet_id anchor",
         "POST", f"{base}/new_comments", None,
         {"file_type": "sheet", "reply_elements": re_elems,
          "anchor": {"sheet_id": sid, "sheet_col": col, "sheet_row": row}}),

        ("v4 new_comments reply_list+anchor",
         "POST", f"{base}/new_comments", None,
         {"file_type": "sheet", "anchor": {"block_id": sid, "sheet_col": col, "sheet_row": row},
          "reply_list": reply_list}),

        ("v5 new_comments file_type query only",
         "POST", f"{base}/new_comments", {"file_type": "sheet"},
         {"reply_elements": re_elems,
          "anchor": {"block_id": sid, "sheet_col": col, "sheet_row": row}}),

        ("v6 new_comments block_id A1",
         "POST", f"{base}/new_comments", None,
         {"file_type": "sheet", "reply_elements": re_elems,
          "anchor": {"block_id": f"{sid}!{a1}"}}),

        ("v7 comments(v1) reply_list sheet anchor",
         "POST", f"{base}/comments", {"file_type": "sheet"},
         {"anchor": {"sheet_id": sid, "sheet_col": col, "sheet_row": row}, "reply_list": reply_list}),

        ("v8 new_comments str col/row",
         "POST", f"{base}/new_comments", None,
         {"file_type": "sheet", "reply_elements": re_elems,
          "anchor": {"block_id": sid, "sheet_col": str(col), "sheet_row": str(row)}}),
    ]

    for v in variants:
        _try(v[0], v[1], v[2], v[3], v[4], H)


if __name__ == "__main__":
    main()
