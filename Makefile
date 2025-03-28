PYTHON_VERSION := $(shell cat .python-version)
export PYTHONPATH := ${PWD}

install-uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh

install-python: install-uv
	uv python install $(PYTHON_VERSION)

setup: install-python
	uv sync --all-groups
	uv run pre-commit install

	# add PYTHONPATH to .env for things like Jupyter
	echo "PYTHONPATH=${PWD}" >> .env


precommit:
	uv run pre-commit run --all-files

format:
	uv run black .

datadownload:
	uv run python scripts/fetch_rule_jsons.py

transform:
	uv run python scripts/process_abilities-scores.py
	uv run python scripts/process_allignments.py
	uv run python scripts/process_conditions.py
	uv run python scripts/process_damage-types.py
	uv run python scripts/process_equipment-categories.py
	uv run python scripts/process_languages.py
	uv run python scripts/process_magic-items.py
	uv run python scripts/process_magic-schools.py
	uv run python scripts/process_skills.py
	uv run python scripts/process_weapon-properties.py
	uv run python scripts/process_equipment.py
	uv run python scripts/process_features.py
	uv run python scripts/process_rule-sections.py
	uv run python scripts/process_rules.py
	uv run python scripts/process_proficiencies.py
	uv run python scripts/process_traits.py
	uv run python scripts/process_subclasses.py



coverage:
	uv run coverage run -m pytest
	uv run coverage report
	uv run coverage-badge -f -o coverage.svg

test:
	pytest
