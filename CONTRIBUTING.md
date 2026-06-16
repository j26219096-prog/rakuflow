# 🤝 Contributing to RakuFlow

Thank you for your interest in contributing to **RakuFlow**! This document outlines how to get started, submit pull requests, and follow our coding standards.

---

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Reporting Issues](#reporting-issues)

---

## Code of Conduct

Please be respectful and constructive in all interactions. We follow the [Contributor Covenant](https://www.contributor-covenant.org/) code of conduct.

---

## Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/rakuflow.git
   cd rakuflow
   ```
3. **Add upstream** remote:
   ```bash
   git remote add upstream https://github.com/j26219096-prog/rakuflow.git
   ```
4. **Create a feature branch**:
   ```bash
   git checkout -b feat/your-feature-name
   ```

---

## Development Setup

### Option A — Local Python (no Docker required for unit tests)

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run unit tests (no Kafka or Postgres needed)
pytest tests/test_producer.py tests/test_simple_consumer.py -v
```

### Option B — Full stack with Docker

```bash
# Copy env file and fill in values
cp .env.example .env

# Start all services
make up

# Run the pipeline
make simple-run
make dbt-run
```

### Option C — GitHub Codespaces (zero install)

Click **"Open in GitHub Codespaces"** in the README — the devcontainer auto-installs all dependencies and forwards all ports.

---

## Project Structure

```
rakuflow/
├── ingestion/          # Kafka producer + Avro-style schemas
├── processing/         # PySpark consumer + simple (psycopg2) consumer
├── dbt_project/        # dbt staging models and mart models
├── dags/               # Airflow DAG definition
├── quality/            # Great Expectations validation suite
├── dashboard/          # Streamlit analytics dashboard
├── tests/              # pytest unit tests
├── scripts/            # Utility scripts (e.g. data generation)
├── docker/             # PostgreSQL init SQL
└── docker-compose.yml  # Full service orchestration
```

---

## Running Tests

```bash
# Unit tests only (no external services)
pytest tests/test_producer.py tests/test_simple_consumer.py -v

# With coverage report
pytest tests/test_producer.py tests/test_simple_consumer.py \
    --cov=ingestion --cov=processing --cov-report=term-missing

# Lint with ruff
ruff check ingestion/ processing/ dags/ quality/ dashboard/ tests/

# All checks (mirrors CI)
make lint && make test
```

---

## Code Style

- **Python**: Follow [PEP 8](https://pep8.org). Formatter: [Black](https://black.readthedocs.io/). Linter: [ruff](https://docs.astral.sh/ruff/).
- **Type hints**: All public functions must have full type annotations.
- **Docstrings**: Use Google-style docstrings for all public functions and classes.
- **SQL (dbt)**: CTE-based SQL with descriptive aliases. One CTE per logical transformation step.
- **Imports**: Standard library → third-party → local (enforced by ruff `I` rules).

Pre-commit setup (optional but recommended):

```bash
pip install pre-commit
pre-commit install
```

---

## Submitting a Pull Request

1. **Sync with upstream** before starting work:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```
2. **Write tests** for any new functionality (see `tests/`).
3. **Run the full test + lint suite** locally:
   ```bash
   make lint && make test
   ```
4. **Commit** with a clear, conventional commit message:
   ```
   feat: add support for multi-topic consumers
   fix: handle null customer_id in simple_consumer
   docs: update dbt model documentation
   test: add edge cases for parse_ts helper
   ```
5. **Push** your branch and open a Pull Request against `main`.
6. Fill in the PR template, link any related issues, and request a review.

CI will automatically run lint + unit tests on every PR. All checks must pass before merging.

---

## Reporting Issues

- Use [GitHub Issues](https://github.com/j26219096-prog/rakuflow/issues) to report bugs or request features.
- Include: Python version, OS, steps to reproduce, expected vs actual behaviour, and relevant log output.
- Label issues appropriately: `bug`, `enhancement`, `documentation`, `question`.

---

<div align="center">
  <strong>Happy coding! ⚡ RakuFlow — Where every order tells a story</strong>
</div>
