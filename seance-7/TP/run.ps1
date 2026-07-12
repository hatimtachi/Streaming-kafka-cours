# Compile et lance une appli Avro de l'atelier (sur votre machine, via Maven).
#
# Usage : .\run.ps1 <producer|consumer> [solution] [v1|v2]
#   .\run.ps1 producer              -> producteur Avro, schema v1 (votre code)
#   .\run.ps1 producer v2           -> producteur Avro, schema v2
#   .\run.ps1 consumer              -> consommateur Avro (votre code)
#   .\run.ps1 producer solution v2  -> corrige, schema v2
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$base = "fr.esgi.kafka.tp7"
$rest = @($args)
$role = if ($rest.Count -ge 1) { $rest[0] } else { "" }
$pkg  = $base
$idx  = 1
if ($rest.Count -ge 2 -and $rest[1] -eq "solution") { $pkg = "$base.solution"; $idx = 2 }
$version = if ($rest.Count -gt $idx) { $rest[$idx] } else { "" }

switch ($role) {
    "producer" { $main = "$pkg.AvroOrderProducer" }
    "consumer" { $main = "$pkg.AvroOrderConsumer" }
    default { Write-Host "Usage : .\run.ps1 <producer|consumer> [solution] [v1|v2]"; exit 1 }
}

if ($version -ne "") {
    mvn -q compile exec:java "-Dexec.mainClass=$main" "-Dexec.args=$version"
} else {
    mvn -q compile exec:java "-Dexec.mainClass=$main"
}
