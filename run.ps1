# AGY-SOC - Notification Priority Reducer
$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

Write-Host "============================================================"
Write-Host " AGY-SOC - Notification Priority Reducer"
Write-Host "============================================================"

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    Write-Error "Java is not installed or not available in PATH."
    exit 1
}

if (-not (Get-Command javac -ErrorAction SilentlyContinue)) {
    Write-Error "Java compiler (JDK) is not installed or not available in PATH."
    exit 1
}

if (-not (Test-Path "bin")) {
    New-Item -ItemType Directory -Path "bin" | Out-Null
}

Write-Host "`n[1/2] Compiling Java source files..."

& javac -d bin src/com/soc/reducer/*.java

if ($LASTEXITCODE -ne 0) {
    Write-Error "Compilation failed."
    exit 1
}

Write-Host "`n[2/2] Starting AGY-SOC server..."
Write-Host "Dashboard: http://localhost:8080/"

Start-Process "http://localhost:8080/"

& java -cp bin com.soc.reducer.SimulationServer 8080