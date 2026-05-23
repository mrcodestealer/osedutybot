import socketio
import time

# 创建客户端，启用引擎日志便于调试
sio = socketio.Client(logger=True, engineio_logger=True)

@sio.event
def connect():
    print('✅ 连接成功')
    # 连接后可能需要发送一些初始消息（如订阅）？先观察

@sio.event
def connect_error(data):
    print(f'❌ 连接错误: {data}')

@sio.event
def disconnect():
    print('❌ 连接断开')

# 监听所有消息
@sio.on('*')
def catch_all(event, data):
    print(f'📨 事件: {event}, 数据: {data}')

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI2NGY4MmFjNWExZjMyODAyMzBmMTA4MmEiLCJhZG1pbk5hbWUiOiJjcG9tMDEiLCJwYXNzd29yZCI6ImhXeWhnMjljT2EyZDNhMjdlMzg5YWMzNTA5OTkwODcyMTI1NzBjZDI0IiwibG9jYWxJcCI6Ijk2LjAuMTQ0LjIzNyIsImlwQWRkcmVzcyI6Ijk2LjAuMTQ0LjIzNywgMTYyLjE1OC4xNjMuMTc1IiwicHVibGljSVAiOiI5Ni4wLjE0NC4yMzciLCJwbGF0Zm9ybXMiOlsiNzk2OGFhNWU1NTM2ZDEzNzQyNTQ1MzgyIl0sImlhdCI6MTc3Njc1MTM5OCwiZXhwIjoxNzc2Nzg3Mzk4fQ.l8eQ-um7-Pn1evmuzM_N_Vqhazq919mPNtsTscPAcTQ"
server_url = f'wss://mgnt-apiserver.casinoplus.top/socket.io/?token={token}&EIO=4'

headers = {
    'Origin': 'https://mgnt-webserver.casinoplus.top',
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
}

try:
    # 不指定 transports，让其自动协商（默认 ['polling', 'websocket']）
    sio.connect(server_url, headers=headers, wait_timeout=10)
    # 保持连接一段时间等待数据推送
    time.sleep(30)
    sio.disconnect()
except Exception as e:
    print(f"连接失败: {e}")