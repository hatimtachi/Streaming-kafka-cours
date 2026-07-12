# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : .\run.ps1 <producer|consumer> [solution]
#   .\run.ps1 producer            -> fr.esgi.kafka.tp5.EventProducer   (votre code)
#   .\run.ps1 consumer            -> fr.esgi.kafka.tp5.GroupConsumer
#   .\run.ps1 consumer solution   -> fr.esgi.kafka.tp5.solution.GroupConsumer (corrige)
#
# Pour le rebalance : lancez ".\run.ps1 consumer" dans 2 ou 3 terminaux.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$base = "fr.esgi.kafka.tp5"
$what = if ($args.Count -ge 1) { $args[0] } else { "" }
$pkg  = if ($args.Count -ge 2 -and $args[1] -eq "solution") { "$base.solution" } else { $base }

switch ($what) {
    "producer" { $main = "$pkg.EventProducer" }
    "consumer" { $main = "$pkg.GroupConsumer" }
    default { Write-Host "Usage : .\run.ps1 <producer|consumer> [solution]"; exit 1 }
}

mvn -q compile exec:java "-Dexec.mainClass=$main"
