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

.PHONY: local-stack-up local-stack-down run-api-gateway run-btc-ingester run-btc-history-sync run-clickhouse-migration lint lint-install lint-docker coverage

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

coverage:
	@echo "Running tests with coverage (coverage.out + coverage.svg)"
	@$(GO) test ./... -covermode=atomic -coverprofile=coverage.out
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {print substr($$3,1,length($$3)-1)}'); \
	pct=$${total%.*}; \
	color="#4c1"; \
	if [ "$$pct" -lt 60 ]; then \
		color="#e05d44"; \
	elif [ "$$pct" -lt 80 ]; then \
		color="#dfb317"; \
	fi; \
	printf '%s\n' \
		"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"120\" height=\"20\" role=\"img\" aria-label=\"Coverage: $${total}%\">" \
		"  <linearGradient id=\"s\" x2=\"0\" y2=\"100%\">" \
		"    <stop offset=\"0\" stop-color=\"#fff\" stop-opacity=\".7\"/>" \
		"    <stop offset=\".1\" stop-opacity=\".1\"/>" \
		"  </linearGradient>" \
		"  <rect rx=\"3\" width=\"120\" height=\"20\" fill=\"#555\"/>" \
		"  <rect rx=\"3\" x=\"60\" width=\"60\" height=\"20\" fill=\"$${color}\"/>" \
		"  <rect rx=\"3\" width=\"120\" height=\"20\" fill=\"url(#s)\"/>" \
		"  <g fill=\"#fff\" text-anchor=\"middle\" font-family=\"DejaVu Sans,Verdana,Geneva,sans-serif\" font-size=\"11\">" \
		"    <text x=\"30\" y=\"14\">coverage</text>" \
		"    <text x=\"90\" y=\"14\">$${total}%</text>" \
		"  </g>" \
		"</svg>" \
		> coverage.svg
