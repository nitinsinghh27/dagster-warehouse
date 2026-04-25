import os
from pathlib import Path

from dotenv import load_dotenv
from dagster_dbt import DbtProject

# Load .env for local development (no-op in CI/prod if file is absent)
load_dotenv(Path(__file__).joinpath("..", "..", ".env").resolve(), override=False)

# DBT_PROJECT_DIR can be overridden in .env to point at a local dbt checkout.
# Default: ./dbt_project next to this package.
_default_project_dir = Path(__file__).joinpath("..", "..", "dbt_project").resolve()
_project_dir = Path(os.environ.get("DBT_PROJECT_DIR", _default_project_dir))

dbt_project = DbtProject(project_dir=_project_dir)

# Compiles the manifest during local `dagster dev` runs;
# in prod the pre-compiled target/manifest.json is expected.
dbt_project.prepare_if_dev()
