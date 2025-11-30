SHELL := /bin/bash
GO ?= go
DOCKER ?= docker

GOLANGCI_LINT_VERSION ?= v2.6.2
GOLANGCI_LINT_BIN ?= $(shell $(GO) env GOPATH)/bin/golangci-lint
GOLANGCI_LINT_IMAGE ?= golangci/golangci-lint:$(GOLANGCI_LINT_VERSION)

LOCAL_DEPLOY_DIR ?= ./deploymens/local
LOCAL_COMPOSE_FILE ?= $(LOCAL_DEPLOY_DIR)/docker-compose.yml

API_GATEWAY_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.api-gateway
BTC_INGESTER_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.btc-ingester
BTC_HISTORY_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.btc-history-ingester
MIGRATIONS_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.clickhouse-migrations

.PHONY: local-stack-up local-stack-down run-api-gateway run-btc-ingester run-btc-history-sync run-clickhouse-migration lint lint-install lint-docker

local-stack-up:
	@echo "Starting local docker compose stack: $(LOCAL_COMPOSE_FILE)"
	docker compose -f $(LOCAL_COMPOSE_FILE) up -d

local-stack-down:
	@echo "Stopping local docker compose stack: $(LOCAL_COMPOSE_FILE)"
	docker compose -f $(LOCAL_COMPOSE_FILE) down

run-clickhouse-migration:
	@echo "Running clickhouse migrations with env file $(MIGRATIONS_ENV)"
	@set -a; \
	source $(MIGRATIONS_ENV); \
	set +a; \
	$(GO) run ./cmd/migrations/clickhouse

lint-docker:
	@echo "Running golangci-lint in Docker image $(GOLANGCI_LINT_IMAGE)"
	@$(DOCKER) run --rm \
		-v $(shell pwd):/app \
		-w /app \
		$(GOLANGCI_LINT_IMAGE) \
		golangci-lint run ./...
