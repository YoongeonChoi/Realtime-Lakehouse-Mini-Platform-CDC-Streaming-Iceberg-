$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:JAVA_TOOL_OPTIONS = ""

$jobList = cmd /c "docker compose exec -T flink-jobmanager /opt/flink/bin/flink list 2>&1"
if ($LASTEXITCODE -eq 0 -and $jobList -match "commerce_medallion_pipeline") {
    Write-Host "Flink job is already running."
    exit 0
}

cmd /c "docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/01_create_catalog_and_tables.sql 2>&1"
