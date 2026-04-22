.PHONY: help install dev-install lint typecheck test test-unit test-bench \
        up down logs topics generate benchmark clean

PYTHON  := python3
PIP     := pip3
PYTEST  := pytest
DOCKER  := docker compose

# Default target
help:
	@echo "StreamGraph — available targets:"
	@echo ""
	@echo "  Setup"
	@echo "    install        Install production dependencies"
	@echo "    dev-install    Install all dependencies including dev tools"
	@echo ""
	@echo "  Quality"
	@echo "    lint           Run ruff linter"
	@echo "    typecheck      Run mypy type checker"
	@echo "    test           Run all unit tests with coverage"
	@echo "    test-unit      Run unit tests only (fast)"
	@echo "    test-bench     Run pytest benchmarks"
	@echo ""
	@echo "  Local Dev"
	@echo "    up             Start full Docker Compose stack"
	@echo "    down           Stop and remove containers"
	@echo "    logs           Tail logs from all services"
	@echo "    topics         Create Kafka topics"
	@echo "    generate       Run generator (stdout, 30 s preview)"
	@echo "    benchmark      Run standalone Python benchmarks"
	@echo ""
	@echo "  Feast"
	@echo "    feast-apply    Apply feature store definitions"
	@echo "    feast-materialize  Materialize features to Redis"
	@echo ""
	@echo "  Cleanup"
	@echo "    clean          Remove build artefacts and caches"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

install:
	$(PIP) install -e .

dev-install:
	$(PIP) install -e ".[dev]"
	pre-commit install

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

lint:
	ruff check src/ generator/ tests/ benchmarks/

typecheck:
	mypy src/streamgraph

test:
	$(PYTEST) tests/ -v --cov=src/streamgraph --cov-report=term-missing

test-unit:
	$(PYTEST) tests/unit/ -v -x

test-bench:
	$(PYTEST) benchmarks/ --benchmark-sort=mean -v

# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------

up:
	$(DOCKER) up -d --build
	@echo ""
	@echo "Services started:"
	@echo "  Kafka UI    : http://localhost:8080"
	@echo "  Flink UI    : http://localhost:8081"
	@echo "  Grafana     : http://localhost:3000  (admin / streamgraph)"
	@echo "  Prometheus  : http://localhost:9090"
	@echo "  Redis       : localhost:6379"

down:
	$(DOCKER) down -v

logs:
	$(DOCKER) logs -f

topics:
	bash scripts/setup_kafka_topics.sh

generate:
	$(PYTHON) -m generator.fraud_ring_generator \
		--stdout \
		--events-per-second 50 \
		--dry-run

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

benchmark:
	PYTHONPATH=src:. $(PYTHON) benchmarks/bench_union_find.py
	PYTHONPATH=src:. $(PYTHON) benchmarks/bench_pipeline.py

# ---------------------------------------------------------------------------
# Feast
# ---------------------------------------------------------------------------

feast-apply:
	cd feast && feast apply

feast-materialize:
	cd feast && feast materialize-incremental $$(date -u +%Y-%m-%dT%H:%M:%S)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage dist build *.egg-info
