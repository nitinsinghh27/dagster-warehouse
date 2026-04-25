"""
Python-side dbt SQL renderer.

Why not `dbt compile`?
- No subprocess race condition on target/manifest.json when 16 assets run in parallel.
- No subprocess latency per asset.
- Full control over is_incremental() — always False, so Python/IOManager controls scope.

How it works:
    1. Read the raw .sql file from dbt_project/models/{layer}/{model_name}.sql.
    2. Render Jinja templates with:
         is_incremental()  → True for bronze incr:/daily:/full (load_date two-phase)
                             False for range:/store:/org: and all silver/gold
         config(**kwargs)  → '' (no-op — dbt materialization config, ignored at runtime)
         ref(name)         → "DATABASE"."LAYER"."TABLE"
         source(src, tbl)  → "INGEST"."DEBEZIUM_STAGING"."TABLE"
         var(name, deflt)  → render_vars.get(name, deflt)
         this              → "DATABASE"."LAYER"."MODEL"  (unused; is_incremental=False)
    3. Return plain executable SQL.

Bronze models have date/store var blocks OUTSIDE is_incremental(), so they filter
the source scan when vars are passed.  Silver/gold models have no var blocks; their
scope is controlled entirely by the IOManager's outer WHERE clause.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from jinja2 import Environment, BaseLoader, Undefined

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model registry: lower-cased model name → layer
# Used by ref() to resolve the target schema.
# ---------------------------------------------------------------------------
MODEL_LAYER: dict[str, str] = {
    # bronze
    "base_pos_ticket":               "bronze",
    "base_pos_ticketline":           "bronze",
    "base_pos_ticketline_tax_log": "bronze",
    "base_inventory":                 "bronze",
    "base_invoice_line_cost":         "bronze",
    "tenant_stores":                               "bronze",
    # silver
    "ticketline_sales":                            "silver",
    "ticket_sales":                                "silver",
    "ticketline_taxes":                            "silver",
    "ticketline_tax_totals":                       "silver",
    "customer_first_tx_timestamp":                 "silver",
    "customer_first_tx":                           "silver",
    "daily_customer_cnt":                          "silver",
    "invoice_line":                                "silver",
    "invoice_fact":                                "silver",
    # gold
    "ra_hourly_sales":                             "gold",
}

# Source database/schema for Debezium CDC staging.
_SOURCE_DB = "INGEST"
_SOURCE_SCHEMA = "DEBEZIUM_STAGING"


def compile_model(
    model_name: str,
    layer: str,
    dbt_project_dir: Path,
    render_vars: dict[str, Any],
    database: str | None = None,
    is_incremental: bool = False,
) -> str:
    """
    Render a dbt model's Jinja SQL to plain executable SQL.

    Args:
        model_name:      Lower-cased dbt model name (e.g. "ticket_sales").
        layer:           "bronze" | "silver" | "gold".
        dbt_project_dir: Path to the dbt project root (contains models/).
        render_vars:     Dict passed to var() — typically includes python_controlled,
                         date_from, date_to, store_ids (bronze only).
        database:        Target Snowflake database.  Defaults to SNOWFLAKE_DATABASE env var.
        is_incremental:  Whether is_incremental() returns True in the Jinja render.
                         True for bronze incr:/daily:/full partitions (load_date pattern).
                         False for range:/store:/org: partitions and all silver/gold.
    """
    if database is None:
        database = os.environ.get("SNOWFLAKE_DATABASE", "NITIN_DAGSTER_POC")

    sql_path = dbt_project_dir / "models" / layer / f"{model_name}.sql"
    logger.debug("Reading SQL file: %s", sql_path)

    if not sql_path.exists():
        raise FileNotFoundError(
            f"dbt model file not found: {sql_path}. "
            "Check model_name spelling and that the file exists under dbt_project/models/{layer}/."
        )

    raw_sql = sql_path.read_text()
    logger.debug("Read %d bytes from %s", len(raw_sql), sql_path.name)
    logger.info(
        "Compiling [%s] %s | vars=%s | target=%s.%s.%s",
        layer, model_name, render_vars,
        database, layer.upper(), model_name.upper(),
    )

    result = _render(raw_sql, model_name, layer, database, render_vars, is_incremental)
    logger.info("Compiled  [%s] %s → %d chars", layer, model_name, len(result))
    return result


def _render(
    raw_sql: str,
    model_name: str,
    layer: str,
    database: str,
    render_vars: dict[str, Any],
    is_incremental: bool = False,
) -> str:
    """Core Jinja rendering logic."""

    target_table = f'"{database}"."{layer.upper()}"."{model_name.upper()}"'

    # ------------------------------------------------------------------ #
    # Jinja context functions                                             #
    # ------------------------------------------------------------------ #

    def ref_fn(name: str) -> str:
        ref_layer = MODEL_LAYER.get(name)
        if ref_layer is None:
            logger.warning(
                "ref('%s') not found in MODEL_LAYER — defaulting to 'bronze'. "
                "Add it to compiler.py MODEL_LAYER to silence this warning.",
                name,
            )
            ref_layer = "bronze"
        resolved = f'"{database}"."{ref_layer.upper()}"."{name.upper()}"'
        logger.debug("  ref('%s') → %s", name, resolved)
        return resolved

    def source_fn(source_name: str, table_name: str) -> str:
        return f'"{_SOURCE_DB}"."{_SOURCE_SCHEMA}"."{table_name.upper()}"'

    def var_fn(name: str, default: Any = None) -> Any:
        return render_vars.get(name, default)

    def config_fn(**kwargs: Any) -> str:
        # dbt config() sets materialization options — no-op at SQL render time.
        return ""

    # ------------------------------------------------------------------ #
    # dbt_utils shim — supports {{ dbt_utils.group_by(n=N) }} from production SQL
    class _DbtUtils:
        def group_by(self, n: int) -> str:
            return "GROUP BY " + ", ".join(str(i) for i in range(1, n + 1))

    # Render                                                              #
    # ------------------------------------------------------------------ #
    env = Environment(loader=BaseLoader())

    template = env.from_string(raw_sql)

    rendered = template.render(
        config=config_fn,
        ref=ref_fn,
        source=source_fn,
        var=var_fn,
        is_incremental=lambda: is_incremental,
        this=target_table,
        dbt_utils=_DbtUtils(),
    )

    return rendered.strip()
