# Lance un outil CLI Kafka dans le conteneur broker-1 (adresse interne broker-1:9092).
# Exemple : .\kcli.ps1 kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic events --partitions 3 --replication-factor 3
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
if ($args.Count -eq 0) {
    Write-Host "Usage : .\kcli.ps1 <outil-kafka.sh> [arguments...]"
    exit 1
}
$tool = $args[0]
if ($args.Count -gt 1) { $rest = $args[1..($args.Count - 1)] } else { $rest = @() }
docker compose exec broker-1 /opt/kafka/bin/$tool @rest
