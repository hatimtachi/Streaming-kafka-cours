# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : .\run.ps1 <producer|consumer> [solution]
#   .\run.ps1 producer            -> app.ProducerApp   (votre code)
#   .\run.ps1 consumer            -> app.ConsumerApp
#   .\run.ps1 producer solution   -> solution.ProducerApp (corrige)
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$what = if ($args.Count -ge 1) { $args[0] } else { "" }
$pkg  = if ($args.Count -ge 2) { $args[1] } else { "app" }

switch ($what) {
    "producer" { $main = "$pkg.ProducerApp" }
    "consumer" { $main = "$pkg.ConsumerApp" }
    default { Write-Host "Usage : .\run.ps1 <producer|consumer> [solution]"; exit 1 }
}

mvn -q compile exec:java "-Dexec.mainClass=$main"
