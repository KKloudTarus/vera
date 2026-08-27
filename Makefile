.DEFAULT_GOAL := help
.PHONY: help install fmt lint typecheck arch test test-int cov check run-api run-worker migrate revision up down clean

# Commands assume the conda env `vera` is active: conda activate vera

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install the project (editable) with all extras into the active env
	python -m pip install -e ".[all]"
	pre-commit install

fmt: ## Format code and apply safe fixes
	ruff format .
	ruff check --fix .

lint: ## Lint without fixing
	ruff check .
	ruff format --check .

typecheck: ## Static type check (pyright strict)
	pyright

arch: ## Enforce clean-architecture import boundaries
	lint-imports

test: ## Run unit tests
	pytest -m "not integration and not llm"

test-int: ## Run integration tests (needs Docker for testcontainers)
	pytest -m integration

cov: ## Run tests with coverage report
	pytest --cov --cov-report=term-missing -m "not integration and not llm"

check: lint typecheck arch test ## Run the full local gate (what CI runs)

run-api: ## Run the API (dev, autoreload)
	uvicorn vera.entrypoints.api.main:app --reload --host 0.0.0.0 --port 8000

run-worker: ## Run the ingestion worker
	python -m vera.entrypoints.worker.main

migrate: ## Apply DB migrations to head
	alembic upgrade head

revision: ## Create a new migration: make revision m="add x"
	alembic revision --autogenerate -m "$(m)"

up: ## Start local infra (postgres, neo4j, valkey, minio)
	docker compose up -d

down: ## Stop local infra
	docker compose down

clean: ## Remove tooling caches
	rm -rf .ruff_cache .pytest_cache .import_linter_cache .coverage htmlcov coverage.xml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
