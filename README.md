# databricks_ecommerce

End-to-end Databricks demo for batch/stream ETL with a Python package, notebooks, and Databricks Asset Bundles for deployment. This README explains local dev vs Databricks compute workflows, installation with `uv`, and how this mono-workspace demo maps to ideal multi-workspace setups.

## Project Layout

- Code: `src/ecommerce` (package entrypoint `ecommerce.main:main`), utilities in `src/utils`.
- Notebooks: `ecommerce_etl/notebooks` (bronze/silver/gold, functions, streaming, extras).
- Scripts: `ecommerce_etl/scripts` mirror notebook steps for jobs/pipelines.
- Bundles config: `databricks.yml` (targets `dev` and `prod`).
- Tests: `tests` with `pytest` config in `pyproject.toml` (`pythonpath=src`).

## Python Tooling (uv + extras)

This repo uses `uv` for dependency management. Optional dependency groups in `pyproject.toml`:

- `dbc`: `databricks-dlt==0.3.0`, `databricks-connect>=15.4,<15.5` for running code from your machine on Databricks compute and accessing remote data (Databricks Connect).
- `dev`: local development and testing: `pytest`, `pyspark==3.5.6`, `ruff`, `pre-commit`, `pytest-cov`.

Key difference and when to use which:

- Use `uv sync --extra dev` for local development. Runs Spark locally with `pyspark`, executes unit tests quickly, uses local data or test fixtures, and avoids Databricks cluster costs. Ideal for: fast iteration, pre-commit linting, CI, and deterministic unit tests.
- Use `uv sync --extra dbc` when you need to execute against Databricks compute and remote data via Databricks Connect. Ideal for: validating with Unity Catalog data, cluster/runtime parity, and interactive development that touches workspace resources. This requires a configured Databricks profile and compatible runtime (see Databricks Connect docs).

## Installation

Prereqs:

- Python 3.11+
- `uv` (https://docs.astral.sh/uv/getting-started/installation/)
- Databricks CLI v0.205+ (bundles) and an authenticated profile (`databricks configure`)

Local dev (fast, no cluster):

1) Create/activate a virtualenv:
   - `uv venv && source .venv/bin/activate`
2) Install dev deps:
   - `uv sync --extra dev`
3) Run tests and lint:
   - `uv run pytest -q`
   - `uv run ruff check .`

Databricks Connect (execute on Databricks compute):

1) Ensure your workspace profile is configured: `databricks configure`.
2) Install dbc deps: `uv sync --extra dbc`.
3) Configure Databricks Connect per your workspace/runtime (cluster policy, runtime version, profile). Once configured, scripts will run on Databricks compute via Connect.

Notes:
- The `dev` extra pins `pyspark==3.5.6` for local Spark. For Connect, your versions must match the target Databricks Runtime compatibility matrix for the selected Connect version.
- Keep only one Spark backend active per session (local PySpark vs Connect) to avoid conflicts.

## Running Locally

- Package entrypoint: `uv run python -m ecommerce.main`.
- Utilities: see `src/utils/datetime_utils.py`; unit tests in `tests/`.
- For notebook exploration locally, open notebooks under `ecommerce_etl/notebooks` (some rely on workspace data; prefer dev fixtures where provided under `fixtures/`).

## Databricks Asset Bundles

This repo uses Databricks Asset Bundles (`databricks.yml`) to define deployable resources and environments.

Common commands:

- Deploy to `dev`: `databricks bundle deploy --target dev`
- Deploy to `prod`: `databricks bundle deploy --target prod`
- Run the default job/pipeline in the current target: `databricks bundle run`
- Preview what will be deployed: `databricks bundle validate`

Behavior and notes:

- Targets map to environments in `databricks.yml`. The `dev` target typically pauses schedules by default; `prod` enables schedules. Inspect `databricks.yml` for resource names and environment-specific overrides.
- The demo uses a single Databricks workspace for both dev and prod targets for simplicity. In production, the ideal setup is a triple-workspace topology (dev + test + prod), each backed by its own bundle target and deployment credentials. The same bundle definition promotes through environments with parameter overrides and permissions per target.
- Notebooks and scripts under `ecommerce_etl` are referenced by jobs/pipelines defined in the bundle. Use the bundle to publish code, notebooks, and configure jobs with cluster policies and permissions.

### Examples: `databricks bundle run`

- Run the default entry for a target (uses `--target dev` by default):
  - `databricks bundle run`

- Run a specific job defined in `databricks.yml` resources:
  - `databricks bundle run --name ecommerce_etl_job`

- Run a DLT pipeline (if defined as a resource):
  - `databricks bundle run --name ecommerce_dlt_pipeline`

- Pass job parameters (tasks with `base_parameters` or `parameters`):
  - `databricks bundle run --name ecommerce_etl_job -- --env dev --run_mode backfill`
    - Everything after `--` is forwarded to the job as parameters. Match keys to your task’s parameter names.

- View recent runs and logs:
  - `databricks bundle runs list --target dev`
  - `databricks bundle runs get --target dev <run-id>`

## CI and Quality

- Pytest configuration in `pyproject.toml` sets `pythonpath=src` and collects tests from `tests/`.
- Coverage is configured to omit `src/ecommerce/*` by default; adjust if you want to include the app entry.
- Ruff and pre-commit hooks are defined for lint/format checks. Run locally via `uv run ruff check .` and `pre-commit run -a` (after `uv sync --extra dev`).

## pyproject.toml Highlights

- `requires-python = ">= 3.11"` and no base runtime dependencies; functionality sits behind extras to keep the core minimal.
- Optional dependency groups:
  - `dbc`: `databricks-dlt`, `databricks-connect` (remote compute, DLT authoring)
  - `dev`: `pytest`, `pyspark`, `ruff`, `pre-commit`, `pytest-cov` (local dev/test)
- Pytest and coverage tools blocks configure test discovery and reports.
- Build system uses Hatch (`hatchling`) with wheel target packaging `src/ecommerce`.
- Console entry point in `[project.scripts]` exposes `main = "ecommerce.main:main"`.

## Typical Workflows

- Local feature work:
  - `uv sync --extra dev`
  - write code + tests, run `uv run pytest -q`
  - lint: `uv run ruff check .`

- Validate against Databricks data/compute:
  - `uv sync --extra dbc`
  - configure Connect to your cluster/runtime
  - run app/tests that require Spark with remote access

- Deploy to workspace via bundle:
  - `databricks bundle deploy --target dev`
  - iterate, then promote using a pull request to `main` to trigger a continuous deployment in prod.

## Workspaces: Demo vs Ideal

- Demo: single workspace with two targets (`dev`, `prod`) in `databricks.yml` to simplify exploration.
- Ideal: three isolated workspaces (dev, test, prod) with separate bundle targets, credentials, and permissions. CI promotes from dev → test → prod using the same bundle definition and environment-specific overrides.

## Troubleshooting

- Version mismatches between `pyspark` (local) and Databricks Connect/Runtime cause import or runtime errors. Align versions per the Databricks compatibility docs.
- If `databricks bundle` fails, run `databricks bundle validate` and confirm your CLI profile and workspace permissions.
- If notebooks reference workspace paths or Unity Catalog objects, provide local test fixtures or guard those cells when running locally.
