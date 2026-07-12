# Interroge le Schema Registry (REST sur localhost:8081).
#
# Sans argument : resume (sujets, versions de orders-value, mode de compatibilite).
# Avec un chemin : .\registry.ps1 subjects/orders-value/versions/2
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$base = "http://localhost:8081"

function Get-Reg($path) {
    try { (Invoke-WebRequest -UseBasicParsing "$base/$path").Content }
    catch { "ERREUR : $($_.Exception.Message)" }
}

if ($args.Count -eq 0) {
    Write-Host "== GET /subjects =="
    Get-Reg "subjects"
    Write-Host "== GET /subjects/orders-value/versions =="
    Get-Reg "subjects/orders-value/versions"
    Write-Host "== GET /config/orders-value  (mode de compatibilite) =="
    Get-Reg "config/orders-value"
} else {
    Get-Reg $args[0]
}
