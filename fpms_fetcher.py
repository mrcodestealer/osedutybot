#!/usr/bin/env python3
"""
本地 Amount Loss 查询服务
"""
import subprocess
import sys
import time
from flask import Flask, jsonify, request

app = Flask(__name__)

cached_result = {"result": None, "timestamp": 0}
CACHE_TTL = 300  # 5分钟

def run_fetcher(date_str=None):
    cmd = [sys.executable, "fpms_fetcher.py"]
    if date_str:
        cmd.append(date_str)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in reversed(lines):
                if 'no amount loss' in line or 'as checked' in line:
                    return line.strip()
            return "查询完成但未获取到有效结果"
        else:
            return f"查询失败: {result.stderr}"
    except Exception as e:
        return f"执行异常: {str(e)}"

@app.route('/amountloss', methods=['GET'])
def amountloss():
    global cached_result
    now = time.time()
    date_param = request.args.get('date')

    if date_param:
        result = run_fetcher(date_param)
        return jsonify({"status": "success", "message": result, "cached": False})

    if cached_result['result'] and (now - cached_result['timestamp']) < CACHE_TTL:
        return jsonify({"status": "success", "message": cached_result['result'], "cached": True})

    result = run_fetcher()
    cached_result = {"result": result, "timestamp": now}
    return jsonify({"status": "success", "message": result, "cached": False})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)