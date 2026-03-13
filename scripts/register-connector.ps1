$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$configPath = Join-Path $root "infra\connect\register-postgres-cdc.json"
$connectorUrl = "http://localhost:8083/connectors/pg-commerce-cdc/config"
$schemaRegistryConfigUrl = "http://localhost:8081/config"

$config = Get-Content $configPath -Raw | ConvertFrom-Json
$connectorConfig = $config.config | ConvertTo-Json -Depth 20

Invoke-RestMethod `
    -Method Put `
    -Uri $connectorUrl `
    -ContentType "application/json" `
    -Body $connectorConfig | Out-Null

Invoke-RestMethod `
    -Method Put `
    -Uri $schemaRegistryConfigUrl `
    -ContentType "application/vnd.schemaregistry.v1+json" `
    -Body '{"compatibility":"BACKWARD_TRANSITIVE"}' | Out-Null

Write-Host "Connector and schema compatibility are configured."
