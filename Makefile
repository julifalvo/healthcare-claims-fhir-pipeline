.PHONY: help up down demo status test test-spark test-dags lint format record clean

AIRFLOW = docker compose exec -T airflow-scheduler airflow
IMAGE_TEST = docker run --rm -v "$(CURDIR)/src:/opt/airflow/project/src" \
	-v "$(CURDIR)/tests:/opt/airflow/project/tests" -v "$(CURDIR)/dags:/opt/airflow/project/dags" \
	-v "$(CURDIR)/pyproject.toml:/opt/airflow/project/pyproject.toml" \
	-e PYTHONPATH=/opt/airflow/project/src -w /opt/airflow/project \
	--entrypoint python claims-airflow:local -m pytest -p no:cacheprovider

help:  ## Show available targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

up:  ## Build and start Airflow, Postgres and the dashboard
	docker compose up -d --build --wait

down:  ## Stop the stack (keeps data volumes)
	docker compose down

demo: up  ## Unpause the DAGs: the first run bootstraps Jan-Sep 2026 automatically
	$(AIRFLOW) dags unpause denials_worklist
	$(AIRFLOW) dags unpause claims_lakehouse
	@echo "Airflow:   http://localhost:8080"
	@echo "Dashboard: http://localhost:8501 (populated when the first run finishes, ~6 min)"

status:  ## Show recent runs
	$(AIRFLOW) dags list-runs claims_lakehouse -o plain | head -5

test: test-spark test-dags  ## All tests inside the Airflow image (no local Java needed)

test-spark:  ## Unit, Spark and end-to-end tests
	$(IMAGE_TEST) tests/unit

test-dags:  ## DAG integrity tests
	$(IMAGE_TEST) tests/dags

lint:  ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

format:  ## Auto-fix lint and formatting
	uv run ruff check --fix .
	uv run ruff format .

record:  ## Re-record README GIFs/screenshots (stack must be running)
	uv run --group demo python scripts/record_demo.py all

clean:  ## Stop the stack and delete all data volumes
	docker compose down -v
