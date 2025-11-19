SHELL := /bin/bash
GO ?= go

LOCAL_DEPLOY_DIR ?= ./deploymens/local
LOCAL_COMPOSE_FILE ?= $(LOCAL_DEPLOY_DIR)/docker-compose.yml

API_GATEWAY_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.api-gateway
BTC_INGESTER_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.btc-ingester
BTC_HISTORY_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.btc-history-ingester
MIGRATIONS_ENV ?= $(LOCAL_DEPLOY_DIR)/envs/.env.clickhouse-migrations

.PHONY: local-stack-up local-stack-down run-api-gateway run-btc-ingester run-btc-history-sync run-clickhouse-migration

local-stack-up:
	@echo "Starting local docker compose stack: $(LOCAL_COMPOSE_FILE)"
	docker compose -f $(LOCAL_COMPOSE_FILE) up -d

local-stack-down:
	@echo "Stopping local docker compose stack: $(LOCAL_COMPOSE_FILE)"
	docker compose -f $(LOCAL_COMPOSE_FILE) down

run-api-gateway:
	@echo "Starting api-gateway with env file $(API_GATEWAY_ENV)"
	@set -a; \
	source $(API_GATEWAY_ENV); \
	set +a; \
	$(GO) run ./cmd/api-gateway

run-btc-ingester:
	@echo "Starting btc ingester with env file $(BTC_INGESTER_ENV)"
	@set -a; \
	source $(BTC_INGESTER_ENV); \
	set +a; \
	$(GO) run ./cmd/btc/ingester

run-btc-history-ingester:
	@echo "Starting btc history ingester with env file $(BTC_HISTORY_ENV)"
	@set -a; \
	source $(BTC_HISTORY_ENV); \
	set +a; \
	$(GO) run ./cmd/btc/history-ingester

run-clickhouse-migration:
	@echo "Running clickhouse migrations with env file $(MIGRATIONS_ENV)"
	@set -a; \
	source $(MIGRATIONS_ENV); \
	set +a; \
	$(GO) run ./cmd/migrations/clickhouse
