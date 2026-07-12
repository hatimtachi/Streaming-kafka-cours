# Affiche les logs des brokers en continu (Ctrl-C pour quitter).
Set-Location $PSScriptRoot
docker compose logs -f
