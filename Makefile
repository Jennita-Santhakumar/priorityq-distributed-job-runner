.PHONY: lint test test-integration migrate-up migrate-down run-server run-worker compose-up compose-down

lint:
	golangci-lint run ./...

test:
	go test -race -short ./...

test-integration:
	go test -race -tags=integration ./tests/integration/...

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

run-server:
	go run ./cmd/server

run-worker:
	go run ./cmd/worker

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
