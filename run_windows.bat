@echo off
call .venv\Scripts\activate.bat
python scripts\run_incremental.py
python scripts\export_ris.py
pause
