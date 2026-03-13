param(
    [int]$Count = 50,
    [int]$IntervalMs = 500
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker compose run --rm event-producer --count $Count --interval-ms $IntervalMs
