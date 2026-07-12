# Lance la suite de tests (TopologyTestDriver, sans cluster).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Write-Host "== Tests (TopologyTestDriver, sans cluster) =="
mvn -q test
