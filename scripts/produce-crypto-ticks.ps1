param(
    [int]$Count = 600,
    [int]$IntervalMs = 10,
    [int]$EventTimeStepMs = 250
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker compose run --rm `
    -e TOPIC=raw.event.market.crypto_ticks_v1 `
    event-producer `
    --mode crypto_tick `
    --count $Count `
    --interval-ms $IntervalMs `
    --event-time-step-ms $EventTimeStepMs
