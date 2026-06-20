@echo off

cls

cd /d "%~dp0"



:: Nome del file di configurazione

set "CONFIG_FILE=last_session.txt"



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



:: 4. Caricamento pulito delle variabili salvate (senza spazi nocivi)

if exist "%CONFIG_FILE%" (

    for /f "usebackq delims=" %%a in ("%CONFIG_FILE%") do set "%%a"

)



:: Se non esistono configurazioni precedenti nel file, imposta i default iniziali

if "%SAVED_MODE%"=="" set "SAVED_MODE=1"

if "%SAVED_TARGET%"=="" set "SAVED_TARGET=autodetect"



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

:: Chiede la scelta proponendo il vecchio valore di SAVED_MODE

set "scelta="

set /p scelta="Scegli un'opzione (1 o 2) [%SAVED_MODE%]: "



:: Se premi INVIO (stringa vuota), eredita il valore memorizzato precedente

if "%scelta%"=="" set "scelta=%SAVED_MODE%"



if "%scelta%"=="1" goto SERIAL_MODE

if "%scelta%"=="2" goto WIFI_MODE

echo [!] Scelta non valida! Riprova.

pause

goto MENU



:SERIAL_MODE

echo.

echo Lascia vuoto per tentare il rilevamento automatico della porta USB.

:: Se la vecchia modalità era Wi-Fi, pulisce il target precedente per non fare confusione

if not "%SAVED_MODE%"=="1" set "SAVED_TARGET=autodetect"



set "com_port="

set /p com_port="Inserisci la porta COM (es. COM3) [%SAVED_TARGET%]: "



:: Se premi INVIO, riprende il target salvato

if "%com_port%"=="" set "com_port=%SAVED_TARGET%"

cls



echo [*] Avvio in modalita SERIALE...

set "MESHTASTIC_MODE=serial"

if "%com_port%"=="autodetect" (set "MESHTASTIC_TARGET=") else (set "MESHTASTIC_TARGET=%com_port%")



:: Scrittura sicura sul file di configurazione senza spazi orfani

(

echo SAVED_MODE=1

echo SAVED_TARGET=%com_port%

) > "%CONFIG_FILE%"

goto START_APP



:WIFI_MODE

echo.

:: Se la vecchia modalità era Seriale, reimposta un IP di esempio generico

if not "%SAVED_MODE%"=="2" set "SAVED_TARGET=192.168.1.250"



set "ip_address="

set /p ip_address="Inserisci l'indirizzo IP del dispositivo [%SAVED_TARGET%]: "



:: Se premi INVIO, riprende l'IP salvato

if "%ip_address%"=="" set "ip_address=%SAVED_TARGET%"



if "%ip_address%"=="" (

    echo [!] L'indirizzo IP e obbligatorio per la modalita Wi-Fi.

    pause

    goto WIFI_MODE

)

cls



echo [*] Avvio in modalita WI-FI (TCP)...

set "MESHTASTIC_MODE=wifi"

set "MESHTASTIC_TARGET=%ip_address%"



:: Scrittura sicura sul file di configurazione senza spazi orfani

(

echo SAVED_MODE=2

echo SAVED_TARGET=%ip_address%

) > "%CONFIG_FILE%"

goto START_APP



:START_APP

:: Lancio pulito dell'applicazione

python -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload

pause 

