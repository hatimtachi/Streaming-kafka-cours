# Pilote Kafka Connect via son API REST (http://localhost:8083). Aucun code.
#
# Usage :
#   .\connect.ps1 deploy connectors\file-source.json   # cree/met a jour (idempotent)
#   .\connect.ps1 list                                 # connecteurs actifs
#   .\connect.ps1 status file-source                   # etat + tasks
#   .\connect.ps1 config file-source                   # config courante
#   .\connect.ps1 delete file-source                   # supprime
#   .\connect.ps1 plugins                              # plugins installes
#   .\connect.ps1                                      # resume
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$base = "http://localhost:8083"
$cmd = if ($args.Count -ge 1) { $args[0] } else { "" }
$arg = if ($args.Count -ge 2) { $args[1] } else { "" }

function Reg($method, $path, $body) {
    try {
        if ($body) {
            (Invoke-WebRequest -UseBasicParsing -Method $method -ContentType "application/json" -Body $body "$base/$path").Content
        } else {
            (Invoke-WebRequest -UseBasicParsing -Method $method "$base/$path").Content
        }
    } catch { "ERREUR : $($_.Exception.Message)" }
}

switch ($cmd) {
    "list"    { Reg GET "connectors" $null }
    "plugins" { Reg GET "connector-plugins" $null }
    "status"  { if (-not $arg) { "Usage : .\connect.ps1 status <nom>"; break }; Reg GET "connectors/$arg/status" $null }
    "config"  { if (-not $arg) { "Usage : .\connect.ps1 config <nom>"; break }; Reg GET "connectors/$arg/config" $null }
    "delete"  { if (-not $arg) { "Usage : .\connect.ps1 delete <nom>"; break }; Reg DELETE "connectors/$arg" $null; "supprime : $arg" }
    "deploy"  {
        if (-not (Test-Path $arg)) { "Fichier introuvable : $arg"; break }
        $json = Get-Content $arg -Raw
        $name = ($json | ConvertFrom-Json).name
        Reg DELETE "connectors/$name" $null | Out-Null
        Start-Sleep -Seconds 1
        Reg POST "connectors" $json
        "deploye : $name"
    }
    ""        { "== GET /connectors =="; Reg GET "connectors" $null }
    default   { "Usage : .\connect.ps1 <deploy FICHIER|list|status NOM|config NOM|delete NOM|plugins>" }
}
