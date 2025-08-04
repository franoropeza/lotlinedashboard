@echo off
:: ---------------------------------------------
:: Ejecuta generar_reporte_incremental.py
:: y guarda un log con fecha AAAAMMDD
:: ---------------------------------------------

:: >>> 1. Ruta a tu instalación de Python  <<< 
set PYTHON_EXE="C:\Users\foropeza\AppData\Local\Programs\Python\Python313\python.exe"

:: >>> 2. Carpeta del proyecto (donde está el .py) <<<
set WORKDIR="C:\Users\foropeza\OneDrive - LOTERIA DE SALTA SA\Documentos\Reporte Billeteras"

:: >>> 3. Carpeta para logs <<<
set LOGDIR=%WORKDIR%\logs

:: crear carpeta de logs si no existe
if not exist %LOGDIR% (
    mkdir %LOGDIR%
)

:: fecha AAAAMMDD
for /f "tokens=1-3 delims=/" %%a in ("%date%") do (
    set DAY=%%a
    set MONTH=%%b
    set YEAR=%%c
)
set FECHA=%YEAR%%MONTH%%DAY%

:: nombre de log
set LOGFILE=%LOGDIR%\reporte_%FECHA%.log

echo ============================= >> %LOGFILE%
echo  Ejecución: %date% %time%     >> %LOGFILE%
echo ============================= >> %LOGFILE%

:: cambiar a la carpeta del proyecto
cd /d %WORKDIR%

:: ejecutar el script y redirigir salida y errores al log
%PYTHON_EXE% "generar_reporte_incremental.py" >> %LOGFILE% 2>&1

echo ---- Fin (%date% %time%) ---- >> %LOGFILE%
