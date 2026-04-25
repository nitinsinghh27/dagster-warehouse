"""
Dagster Definitions — wires all assets, jobs, schedules, and resources.

Architecture overview
─────────────────────
Assets (16 total, one per table):
  Bronze (6) → Silver (9) → Gold (1)
  Each asset compiles its dbt model SQL via Python Jinja renderer and returns
  a plain SQL SELECT string.  No dbt subprocess, no data through Python memory.

IOManager (WarehouseSnowflakeIOManager):
  Receives the compiled SQL string, creates a Snowflake TEMP TABLE, then writes
  to the permanent target table using a strategy driven by the partition key:

    "incr:DATE"    → MERGE last 2 days   (3-hourly, handles C + U ops)
    "daily:DATE"   → MERGE last 30 days  (nightly, catches late-arriving U ops)
    "range:DATE:N" → MERGE last N days   (on-demand repair)
    "full"         → TRUNCATE + INSERT   (monthly, clears D op drift)
    "store:ID"     → DELETE + INSERT     (per-store surgical refresh)
    "org:ID"       → DELETE + INSERT     (per-org surgical refresh)

Jobs / Schedules:
  incremental_job    — every 3 hours, MERGE 2 days
  daily_refresh_job  — nightly, MERGE 30 days
  monthly_full_job   — 1st of month, TRUNCATE + INSERT

Pydantic schema validation:
  Registered for load_input only (when downstream Python assets read data back).
  The write path is Snowflake-side — data never enters Python memory.
"""

import os

from dagster import Definitions, load_assets_from_modules

from warehouse_orchestrator.assets import bronze, silver, gold
from warehouse_orchestrator.io_managers.snowflake import WarehouseSnowflakeIOManager
from warehouse_orchestrator.jobs.warehouse import (
    incremental_job,
    incremental_schedule,
    weekly_refresh_job,
    weekly_refresh_schedule,
    monthly_full_job,
    monthly_full_schedule,
)
from warehouse_orchestrator.resources.snowflake import build_snowflake_resource
from warehouse_orchestrator.schemas.bronze import BRONZE_SCHEMA_MAP
from warehouse_orchestrator.schemas.silver import SILVER_SCHEMA_MAP
from warehouse_orchestrator.schemas.gold import GOLD_SCHEMA_MAP

# ---------------------------------------------------------------------------
# IOManager
# ---------------------------------------------------------------------------
_database = os.environ.get("SNOWFLAKE_DATABASE", "NITIN_DAGSTER_POC")

_io_manager = WarehouseSnowflakeIOManager(database=_database)

for _name, _cls in {**BRONZE_SCHEMA_MAP, **SILVER_SCHEMA_MAP, **GOLD_SCHEMA_MAP}.items():
    _io_manager.register_schema(_name, _cls)

# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------
defs = Definitions(
    assets=load_assets_from_modules([bronze, silver, gold]),
    jobs=[
        incremental_job,
        weekly_refresh_job,
        monthly_full_job,
    ],
    schedules=[
        incremental_schedule,
        weekly_refresh_schedule,
        monthly_full_schedule,
    ],
    resources={
        "snowflake": build_snowflake_resource(),
        "warehouse_io_manager": _io_manager,
    },
)
