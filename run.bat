@echo off
cls
cd /d "%~dp0"

:: 1. Controllo e creazione Ambiente Virtuale se non esiste
if not exist meshtastic_env (
    echo [*] Creazione ambiente virtuale in corso...
    python -m venv meshtastic_env
)

:: 2. Attivazione ambiente virtuale
call meshtastic_env\Scripts\activate.bat

:: 3. Controllo/Installazione rapida delle dipendenze
echo [*] Verifica delle librerie in corso...
pip install meshtastic fastapi uvicorn "uvicorn[standard]" >nul 2>&1

:MENU
cls
echo =======================================================
echo          MESHTASTIC CLIENT - SELEZIONE INTERFACCIA
echo =======================================================
echo.
echo  [1] Connessione tramite Cavo Seriale (USB)
echo  [2] Connessione tramite Rete Wi-Fi (TCP)
echo.
echo =======================================================
set /p scelta="Scegli un'opzione (1 o 2): "

if "%scelta%"=="1" goto SERIAL_MODE
if "%scelta%"=="2" goto WIFI_MODE
echo [!] Scelta non valida! Riprova.
pause
goto MENU

:SERIAL_MODE
echo.
echo Lascia vuoto per tentare il rilevamento automatico della porta USB.
set /p com_port="Inserisci la porta COM (es. COM3): "
cls
echo [*] Avvio in modalita SERIALE...
set MESHTASTIC_MODE=serial
set MESHTASTIC_TARGET=%com_port%
goto START_APP

:WIFI_MODE
echo.
set /p ip_address="Inserisci l'indirizzo IP del dispositivo LoRa (es. 192.168.1.250): "
if "%ip_address%"=="" (
    echo [!] L'indirizzo IP e obbligatorio per la modalita Wi-Fi.
    pause
    goto WIFI_MODE
)
cls
echo [*] Avvio in modalita WI-FI (TCP)...
set MESHTASTIC_MODE=wifi
set MESHTASTIC_TARGET=%ip_address%
goto START_APP

:START_APP
:: Lancio pulito di uvicorn senza argomenti extra che generano errori
python -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
pause