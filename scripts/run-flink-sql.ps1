$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$jobList = docker compose exec -T flink-jobmanager /opt/flink/bin/flink list 2>$null
if ($LASTEXITCODE -eq 0 -and $jobList -match "commerce_medallion_pipeline") {
    Write-Host "Flink job is already running."
    exit 0
}

docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/01_create_catalog_and_tables.sql
