@echo off
title AGY-SOC Notification Priority Reducer
cd /d "%~dp0"

echo ================================================================
echo AGY-SOC - Notification Priority Reducer
echo ================================================================

where java >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Java is not installed or not in PATH.
    pause
    exit /b 1
)

where javac >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Java compiler (JDK) is not installed or not in PATH.
    pause
    exit /b 1
)

if not exist bin mkdir bin

echo.
echo [1/2] Compiling Java source files...

javac -d bin src\com\soc\reducer\*.java

if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Compilation failed.
    pause
    exit /b 1
)

echo.
echo [2/2] Starting AGY-SOC server...
echo Dashboard: http://localhost:8080/
echo.

start "" "http://localhost:8080/"

java -cp bin com.soc.reducer.SimulationServer 8080

pause