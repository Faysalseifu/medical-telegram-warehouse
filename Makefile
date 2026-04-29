SHELL := /bin/bash
PYTHON ?= python

.PHONY: help setup install run-scraper run-load-raw run-yolo run-dbt dbt-debug dbt-test run-api run-dagster test docker-up docker-down clean

help:
	@echo "Targets: setup, install, run-scraper, run-load-raw, run-yolo, run-dbt, dbt-debug, dbt-test, run-api, run-dagster, test, docker-up, docker-down, clean"

setup:
	$(PYTHON) -m venv .venv
	@echo "Activate with: source .venv/Scripts/activate"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

run-scraper:
	$(PYTHON) src/scraper.py

run-load-raw:
	$(PYTHON) src/load_raw.py

run-yolo:
	$(PYTHON) src/yolo_detect.py

run-dbt:
	cd medical_warehouse && dbt run --select staging marts

dbt-debug:
	cd medical_warehouse && dbt debug

dbt-test:
	cd medical_warehouse && dbt test

run-api:
	uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

run-dagster:
	dagster dev -f dagster_project/definitions.py

test:
	pytest

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down

clean:
	rm -rf .venv __pycache__ .pytest_cache
