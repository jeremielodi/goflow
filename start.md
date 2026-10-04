go mod init github.com/jeremielodi/goflow
go mod tidy

docker compose -f docker-compose.yaml -f docker-compose.citus.yml down -v 2>&1 | tail -20 && docker compose up -d --wait 2>&1 | tail -15


docker compose ps --format "table {{.ID}}\t{{.Name}}\t{{.Status}}"