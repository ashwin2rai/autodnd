PYTHON_VERSION := $(shell cat .python-version)

install-uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh

install-python: install-uv
	uv python install $(PYTHON_VERSION)

setup: install-python
	uv sync --all-groups
	uv run pre-commit install

	# add PYTHONPATH to .env for things like Jupyter
	echo "PYTHONPATH=${PWD}" >> .env

pathexport:
	export PYTHONPATH=${PWD}

precommit:
	uv run pre-commit run --all-files

format:
	uv run black .

transform: pathexport
	uv run python scripts/process_abilities-scores.py

coverage:
	uv run coverage run -m pytest
	uv run coverage report
	uv run coverage-badge -f -o coverage.svg

test:
	pytest
