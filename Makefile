line_length = 95

.PHONY: fmt
fmt: ## Format code with black and isort
	black . -t py37 --line-length=${line_length}
	isort .
	black . --check -t py37 --line-length=${line_length}

.PHONY: lint
lint: ## Run linters
	mypy kedro_dolt
	flake8 . --max-line-length=${line_length} --per-file-ignores='__init__.py:F401'

.PHONY: lint
test: ## Run tests
	pytest tests --cov=kedro_dolt --cov-report=term --cov-report xml
