"""
Gold layer assets — one @asset per table.

ra_hourly_sales is an aggregation over silver ticket_sales, customer_first_tx,
ticketline_tax_totals, invoice_fact, and the tenant dimension.

partition_expr = "hour_close" — the IOManager's outer WHERE filters by
hour_close::DATE BETWEEN date_from AND date_to so only the relevant hourly
rows enter the temp table before the MERGE.

For date-based partitions the MERGE correctly handles aggregated rows whose
value may decrease (returns, price corrections) because the compiled SQL
recomputes the full aggregation for the date range, and MERGE UPDATE SET
overwrites the old aggregated values entirely.
"""

import os
from pathlib import Path

import dagster as dg

from warehouse_orchestrator.assets.silver import (
    customer_first_tx,
    invoice_fact,
    ticket_sales,
    ticketline_tax_totals,
)
from warehouse_orchestrator.assets.bronze import tenant_stores
from warehouse_orchestrator.compiler import compile_model
from warehouse_orchestrator.partitions import warehouse_partitions

_DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"
_GOLD_VARS = {"python_controlled": True}


@dg.asset(
    key_prefix=["warehouse", "gold"],
    partitions_def=warehouse_partitions,
    deps=[ticket_sales, customer_first_tx, ticketline_tax_totals, invoice_fact, tenant_stores],
    metadata={
        "partition_expr": "hour_close",
        "unique_keys": ["synth_key"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="gold",
    description=(
        "Retail Analytics hourly sales report. "
        "Grain: store × hour × ticket_type × revenue_source × customer_type "
        "× customer_visit_type × is_sample × is_promo. "
        "synth_key is an MD5 over all grain columns."
    ),
)
def ra_hourly_sales(context: dg.AssetExecutionContext) -> str:
    context.log.info(
        "[gold] ra_hourly_sales | partition=%s | date scope applied by IOManager outer WHERE",
        context.partition_key,
    )
    return compile_model(
        model_name="ra_hourly_sales",
        layer="gold",
        dbt_project_dir=_DBT_PROJECT_DIR,
        render_vars=_GOLD_VARS,
        database=os.environ.get("SNOWFLAKE_DATABASE", "NITIN_DAGSTER_POC"),
    )
