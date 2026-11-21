@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo [1/2] 檢查並安裝必要套件 (websockets, aiohttp, requests)...
python -m pip install -r requirements.txt
echo.
echo [2/2] 啟動 Polymarket ETH 5m 模擬盤 (預設本金 1000 U / 單筆 25 U)...
python run_monitor.py --db data/live_eth_5m.db --bankroll 1000 --bet 25
pause
