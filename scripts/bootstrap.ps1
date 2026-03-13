$ErrorActionPreference = "Stop"

function Wait-ForHttp {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][string]$Name,
        [int]$MaxAttempts = 60
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
            Write-Host "$Name is ready: $Url"
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "$Name did not become ready in time."
}

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

docker compose up -d --build

Wait-ForHttp -Url "http://localhost:8081/subjects" -Name "Schema Registry"
Wait-ForHttp -Url "http://localhost:8083/connectors" -Name "Kafka Connect"
Wait-ForHttp -Url "http://localhost:8080/v1/info" -Name "Trino"
Wait-ForHttp -Url "http://localhost:8082/overview" -Name "Flink"

& "$PSScriptRoot\register-connector.ps1"
& "$PSScriptRoot\run-flink-sql.ps1"

Write-Host "Bootstrap complete."
