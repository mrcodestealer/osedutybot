# Local Duty Bot: Ollama + Lark WebSocket long connection
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".env")) {
    Write-Host "Missing .env — copy from server or: copy .env.local.example .env" -ForegroundColor Yellow
    exit 1
}

Write-Host "Checking Ollama..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Uri "http://127.0.0.1:11434/api/tags" -TimeoutSec 5 | Out-Null
} catch {
    Write-Host "Start Ollama from the system tray first." -ForegroundColor Red
    exit 1
}

$model = (Select-String -Path ".env" -Pattern "^BOT_CHAT_MODEL=(.+)$").Matches.Groups[1].Value
if ($model) {
    Write-Host "Pulling model if needed: $model" -ForegroundColor Cyan
    ollama pull $model
}

Write-Host "Installing Python deps..." -ForegroundColor Cyan
.\.venv\Scripts\python.exe -m pip install -r requirements-local.txt

Write-Host "Starting Duty Bot (Flask + Lark long connection)..." -ForegroundColor Green
.\.venv\Scripts\python.exe run_local_bot.py
