# Lance un outil CLI Kafka A L'INTERIEUR du conteneur broker-1.
#
# A l'interieur du reseau Docker, les 3 brokers s'adressent par broker-N:29092.
# C'est cette adresse qu'il faut donner en --bootstrap-server.
#
# Exemples :
#   .\kcli.ps1 kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes
#   .\kcli.ps1 kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --producer-property acks=all
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if ($args.Count -eq 0) {
    Write-Host "Usage : .\kcli.ps1 <outil-kafka.sh> [arguments...]"
    Write-Host "Exemple : .\kcli.ps1 kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes"
    exit 1
}

$tool = $args[0]
if ($args.Count -gt 1) { $rest = $args[1..($args.Count - 1)] } else { $rest = @() }

docker compose exec broker-1 /opt/kafka/bin/$tool @rest
