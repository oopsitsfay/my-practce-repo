@echo off
REM Filter wallets.csv down to wallets with 4+ transactions (in or out).
REM
REM   run.bat https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
REM
REM Results: out\passed.csv (keep) and out\filtered.csv (cut).
REM Safe to re-run -- it resumes where it left off.
setlocal
cd /d "%~dp0"

set "URL=%~1"
if "%URL%"=="" set "URL=%ALCHEMY_URL%"
if "%URL%"=="" (
    echo usage: run.bat ^<alchemy-url^>   ^(or set ALCHEMY_URL^)
    exit /b 2
)

python -c "import requests" 2>NUL || python -m pip install --quiet requests

python filter_wallets.py wallets.csv --min-tx 4 --url "%URL%"
