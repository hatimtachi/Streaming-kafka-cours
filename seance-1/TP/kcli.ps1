# Lance un outil CLI Kafka A L'INTERIEUR du conteneur broker.
#
# Exemples :
#   .\kcli.ps1 kafka-topics.sh --bootstrap-server localhost:9092 --list
#   .\kcli.ps1 kafka-console-producer.sh --bootstrap-server localhost:9092 --topic premier-topic
#   .\kcli.ps1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic premier-topic --from-beginning
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if ($args.Count -eq 0) {
    Write-Host "Usage : .\kcli.ps1 <outil-kafka.sh> [arguments...]"
    Write-Host "Exemple : .\kcli.ps1 kafka-topics.sh --bootstrap-server localhost:9092 --list"
    exit 1
}

$tool = $args[0]
if ($args.Count -gt 1) { $rest = $args[1..($args.Count - 1)] } else { $rest = @() }

docker compose exec broker /opt/kafka/bin/$tool @rest
