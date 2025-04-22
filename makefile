.PHONY: install setup set-precommit check-quality set-style

setup: install set-precommit


install:
	conda install --yes tmux=3.3a python-dotenv=0.21.0 loguru=0.7.2 ruff=0.11.4 pre-commit=3.4.0

set-precommit:
	pre-commit install
	git config --local commit.template .gitmessage

check-quality:
	ruff check .
	ruff format --check .

set-style:
	ruff check --fix .
	ruff format .
