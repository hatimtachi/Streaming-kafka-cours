# Compile et lance l'application du TP (necessite JDK 21 + Maven).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Write-Host "== Application : compilation + execution =="
mvn -q compile exec:java "-Dexec.mainClass=fr.esgi.kafka.tp12.WindowAndJoin"
