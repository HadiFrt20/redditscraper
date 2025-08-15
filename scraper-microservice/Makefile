# Minimal Makefile

PY  := python3
PIP := $(PY) -m pip

.PHONY: deps test lint format check help

deps:
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt

test:
	PYTHONPATH=. pytest -q --verbose

lint:
	ruff check .

format:
	black .

check:
	black --check .
	ruff check .

help:
	@echo "make deps    # upgrade pip, install requirements + dev tools"
	@echo "make test    # run pytest"
	@echo "make lint    # ruff lint"
	@echo "make format  # black format"
	@echo "make check   # black --check + ruff"
