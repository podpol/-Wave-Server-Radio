@echo off
:: Anti-flash guard — see install.bat for why this is here.
if not "%~1"=="RELAUNCHED" (
    start "Wave Server" cmd /k "%~f0" RELAUNCHED
    exit /b
)

chcp 65001 >nul
cd /d "%~dp0"
title Wave 🌊 — Lighthouse Server

color 0E

echo.
echo   ╔══════════════════════════════════════════════════════════════════╗
echo   ║                                                                  ║
echo   ║     🌊 Wave — Lighthouse Signaling Server                         ║
echo   ║     Find your wave. Be on the same wave.                         ║
echo   ║                                                                  ║
echo   ╚══════════════════════════════════════════════════════════════════╝
echo.

:: Check Node.js
node --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo   ⚠️  [ERROR] Node.js is not installed.
    echo   📥 Download: https://nodejs.org
    echo.
    pause
    exit /b 1
)

:: Check dependencies (always, not just "if missing" — otherwise an upgraded
:: server.js/federation.js that needs new packages like helmet/dotenv will
:: silently run without them if node_modules already exists from before)
echo   ⛵ [INFO] Checking dependencies...
call npm install --no-fund --no-audit >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo   ❌ [ERROR] npm install failed. Run install.bat first, or check your internet connection.
    echo.
    pause
    exit /b 1
)
echo   ✅ Dependencies OK
echo.

:: Check .env
if not exist ".env" (
    echo   ⚠️  [WARNING] No .env file found — the server will start with
    echo      INSECURE DEFAULT secrets ^(anyone can send bot messages, no
    echo      federation secret set^). Run install.bat once to generate one,
    echo      or create .env manually. Continuing in 5 seconds anyway...
    echo.
    timeout /t 5 >nul
)

:: Show local IPs
echo   ┌──────────────────────────────────────────────────────────────────┐
echo   │  🌐 Local network URLs (share with others on your network)        │
echo   └──────────────────────────────────────────────────────────────────┘
echo.
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /C:"IPv4"') do (
    for /f "tokens=1" %%b in ("%%a") do (
        echo     🏠 ws://%%b:3000        (клиенты)
        echo     🕸️  ws://%%b:3001        (федерация — для других серверов сети)
    )
)
echo.
echo   ┌──────────────────────────────────────────────────────────────────┐
echo   │  ☁️  Internet URLs (если порты проброшены на роутере)             │
echo   └──────────────────────────────────────────────────────────────────┘
echo     🌍 ws://YOUR_PUBLIC_IP:3000   — сюда подключаются клиенты Wave
echo     🌍 ws://YOUR_PUBLIC_IP:3001   — этот адрес давай другим как
echo                                     FEDERATION_BOOTSTRAP, чтобы
echo                                     твой сервер попал в общую сеть
echo.
echo   ┌──────────────────────────────────────────────────────────────────┐
echo   │  📊 Status Page                                                   │
echo   └──────────────────────────────────────────────────────────────────┘
echo     📈 http://localhost:3000
echo.
echo   ╔══════════════════════════════════════════════════════════════════╗
echo   ║  🚀 Server is setting sail...                                     ║
echo   ║  Press Ctrl+C to drop anchor and stop.                           ║
echo   ╚══════════════════════════════════════════════════════════════════╝
echo.

:: Start server
node server.js

:: If server crashed
echo.
echo   ╔══════════════════════════════════════════════════════════════════╗
echo   ║  ⚓ Server stopped unexpectedly.                                  ║
echo   ║  Check the logs above for errors.                                ║
echo   ╚══════════════════════════════════════════════════════════════════╝
echo.
pause
