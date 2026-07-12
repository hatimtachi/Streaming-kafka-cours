# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : .\run.ps1 <seed|pipeline> [solution]
#   .\run.ps1 seed                -> fr.esgi.kafka.tp6.OrderSeeder   (alimente "orders")
#   .\run.ps1 pipeline            -> fr.esgi.kafka.tp6.EosPipeline   (votre code)
#   .\run.ps1 pipeline solution   -> fr.esgi.kafka.tp6.solution.EosPipeline (corrige)
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$base = "fr.esgi.kafka.tp6"
$what = if ($args.Count -ge 1) { $args[0] } else { "" }
$pkg  = if ($args.Count -ge 2 -and $args[1] -eq "solution") { "$base.solution" } else { $base }

switch ($what) {
    "seed"     { $main = "$base.OrderSeeder" }
    "pipeline" { $main = "$pkg.EosPipeline" }
    default { Write-Host "Usage : .\run.ps1 <seed|pipeline> [solution]"; exit 1 }
}

mvn -q compile exec:java "-Dexec.mainClass=$main"
