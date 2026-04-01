#!/usr/bin/env python3
"""
读取本地 CSV 文件中的请假记录（仅显示 Approved 状态）
"""

import sys
import csv

CSV_FILE = "leave_data.csv"

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--csv":
        output_csv = True
    else:
        output_csv = False

    try:
        with open(CSV_FILE, newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            # 找到需要的列索引
            needed = ["Name", "Leave Type", "Start Date", "End Date", "Reason", "Status"]
            col_index = {}
            for i, col in enumerate(header):
                if col in needed:
                    col_index[col] = i

            if "Status" not in col_index:
                print("未找到 Status 列")
                return

            # 过滤 Approved 记录
            approved_rows = []
            for row in reader:
                if len(row) > col_index["Status"] and row[col_index["Status"]].strip().lower() == "approved":
                    name = row[col_index["Name"]] if col_index["Name"] < len(row) else ""
                    leave_type = row[col_index["Leave Type"]] if col_index["Leave Type"] < len(row) else ""
                    start_date = row[col_index["Start Date"]] if col_index["Start Date"] < len(row) else ""
                    end_date = row[col_index["End Date"]] if col_index["End Date"] < len(row) else ""
                    reason = row[col_index["Reason"]] if col_index["Reason"] < len(row) else ""
                    approved_rows.append([name, leave_type, start_date, end_date, reason])

            if not approved_rows:
                print("没有已批准的请假记录")
                return

            # 输出
            columns = ["Name", "Leave Type", "Start Date", "End Date", "Reason"]
            if output_csv:
                writer = csv.writer(sys.stdout)
                writer.writerow(columns)
                writer.writerows(approved_rows)
            else:
                # 计算列宽
                col_widths = [len(col) for col in columns]
                for row in approved_rows:
                    for i, cell in enumerate(row):
                        col_widths[i] = max(col_widths[i], len(cell))

                def print_sep():
                    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

                print_sep()
                header_line = "| " + " | ".join(c.ljust(w) for c, w in zip(columns, col_widths)) + " |"
                print(header_line)
                print_sep()
                for row in approved_rows:
                    line = "| " + " | ".join(cell.ljust(w) for cell, w in zip(row, col_widths)) + " |"
                    print(line)
                print_sep()
                print(f"总共 {len(approved_rows)} 条已批准的请假记录")

    except FileNotFoundError:
        print(f"错误：找不到文件 {CSV_FILE}，请确保已将 Lark 表格导出为 CSV 并放在当前目录。")
    except Exception as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    main()