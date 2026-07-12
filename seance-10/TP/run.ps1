# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : .\run.ps1 <app|seed> [solution]
#   .\run.ps1 app             -> fr.esgi.kafka.tp10.OrderRouterApp   (votre topologie ; tourne en continu)
#   .\run.ps1 app solution    -> fr.esgi.kafka.tp10.solution.OrderRouterApp (corrige)
#   .\run.ps1 seed            -> fr.esgi.kafka.tp10.OrderProducer    (envoie des commandes de demo)
#
# Astuce parallelisme : lancez ".\run.ps1 app" dans DEUX terminaux (meme application.id).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$base = "fr.esgi.kafka.tp10"
$what = if ($args.Count -ge 1) { $args[0] } else { "" }
$pkg  = if ($args.Count -ge 2 -and $args[1] -eq "solution") { "$base.solution" } else { $base }

switch ($what) {
    "app"  { $main = "$pkg.OrderRouterApp" }
    "seed" { $main = "$base.OrderProducer" }
    default { Write-Host "Usage : .\run.ps1 <app|seed> [solution]"; exit 1 }
}

mvn -q compile exec:java "-Dexec.mainClass=$main"
