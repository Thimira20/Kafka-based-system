# Creates the topics used by the pipeline. Run after `docker compose up -d`.
# Usage: .\scripts\create-topics.ps1

$broker = "kafka"
$bootstrap = "localhost:9092"

function New-Topic {
    param(
        [string]$Name,
        [int]$Partitions
    )
    Write-Host "Creating topic '$Name' (partitions=$Partitions)..."
    docker exec $broker kafka-topics `
        --bootstrap-server $bootstrap `
        --create --if-not-exists `
        --topic $Name `
        --partitions $Partitions `
        --replication-factor 1
}

New-Topic -Name "orders" -Partitions 3
New-Topic -Name "orders.DLQ" -Partitions 1

Write-Host ""
Write-Host "Topics now on the cluster:"
docker exec $broker kafka-topics --bootstrap-server $bootstrap --list
