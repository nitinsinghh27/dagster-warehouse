"""
Silver layer assets — one @asset per table.

Silver models reference upstream bronze Snowflake tables directly (via ref()).
They have no var blocks outside is_incremental(), so compile vars only include
python_controlled=True.  The IOManager adds the outer WHERE clause to scope the
temp table to the partition's date range or store/org.

Dependency declarations mirror the dbt ref() graph so Dagster enforces the
correct execution order: bronze assets complete before silver assets run.

partition_expr drives the IOManager's outer WHERE filter.
customer_first_tx_timestamp has no date column — it processes all rows on every
run (MERGE ALL).  This is a small aggregation table so the cost is negligible.
"""

import os
from pathlib import Path

import dagster as dg

from warehouse_orchestrator.assets.bronze import (
    base_pos_ticketline_tax_log,
    base_pos_ticket,
    base_pos_ticketline,
    base_invoice_line_cost,
    base_inventory,
    tenant_stores,
)
from warehouse_orchestrator.compiler import compile_model
from warehouse_orchestrator.partitions import warehouse_partitions

_DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"
_SILVER_VARS = {"python_controlled": True}


def _compile(model_name: str, context: dg.AssetExecutionContext) -> str:
    context.log.info(
        "[silver] %s | partition=%s | date scope applied by IOManager outer WHERE",
        model_name, context.partition_key,
    )
    return compile_model(
        model_name=model_name,
        layer="silver",
        dbt_project_dir=_DBT_PROJECT_DIR,
        render_vars=_SILVER_VARS,
        database=os.environ.get("SNOWFLAKE_DATABASE", "NITIN_DAGSTER_POC"),
    )


# ---------------------------------------------------------------------------
# ticketline_sales
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[base_pos_ticketline, base_pos_ticket, tenant_stores],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticketline_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Enriched ticket line items with tenant dimensions, is_sample/is_promo flags.",
)
def ticketline_sales(context: dg.AssetExecutionContext) -> str:
    return _compile("ticketline_sales", context)


# ---------------------------------------------------------------------------
# ticket_sales
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[dg.AssetKey(["warehouse", "silver", "ticketline_sales"])],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticket_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Ticket-level aggregation — units, revenue, discounts, returns, net_sales.",
)
def ticket_sales(context: dg.AssetExecutionContext) -> str:
    return _compile("ticket_sales", context)


# ---------------------------------------------------------------------------
# ticketline_taxes
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[base_pos_ticketline_tax_log, dg.AssetKey(["warehouse", "silver", "ticketline_sales"])],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "tax_log_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Tax events joined to ticketline dimensions.",
)
def ticketline_taxes(context: dg.AssetExecutionContext) -> str:
    return _compile("ticketline_taxes", context)


# ---------------------------------------------------------------------------
# ticketline_tax_totals
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[dg.AssetKey(["warehouse", "silver", "ticketline_taxes"])],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticket_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Ticket-level tax totals — total, excise, sales_tax.",
)
def ticketline_tax_totals(context: dg.AssetExecutionContext) -> str:
    return _compile("ticketline_tax_totals", context)


# ---------------------------------------------------------------------------
# customer_first_tx_timestamp
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[base_pos_ticket],
    metadata={
        # No partition_expr — full MERGE ALL on every run.
        # This is intentional: a new ticket could be the earliest for any customer.
        "unique_keys": ["store_id", "customer_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="First completed-ticket timestamp per customer per store.",
)
def customer_first_tx_timestamp(context: dg.AssetExecutionContext) -> str:
    return _compile("customer_first_tx_timestamp", context)


# ---------------------------------------------------------------------------
# customer_first_tx
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[
        base_pos_ticket,
        dg.AssetKey(["warehouse", "silver", "customer_first_tx_timestamp"]),
    ],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticket_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Visit classification — new / returning / walk_in per ticket.",
)
def customer_first_tx(context: dg.AssetExecutionContext) -> str:
    return _compile("customer_first_tx", context)


# ---------------------------------------------------------------------------
# daily_customer_cnt
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[dg.AssetKey(["warehouse", "silver", "ticket_sales"])],
    metadata={
        "partition_expr": "sale_date",
        "unique_keys": ["store_id", "sale_date"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Unique customer count per store per day.",
)
def daily_customer_cnt(context: dg.AssetExecutionContext) -> str:
    return _compile("daily_customer_cnt", context)


# ---------------------------------------------------------------------------
# invoice_line
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[
        base_invoice_line_cost,
        base_inventory,
        dg.AssetKey(["warehouse", "silver", "ticketline_sales"]),
    ],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "invoice_line_cost_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Cost data joined to ticketline for COGS attribution.",
)
def invoice_line(context: dg.AssetExecutionContext) -> str:
    return _compile("invoice_line", context)


# ---------------------------------------------------------------------------
# invoice_fact
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "silver"],
    partitions_def=warehouse_partitions,
    deps=[dg.AssetKey(["warehouse", "silver", "invoice_line"])],
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticket_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="silver",
    description="Ticket-level COGS aggregation — 4 cost types + sample/promo splits.",
)
def invoice_fact(context: dg.AssetExecutionContext) -> str:
    return _compile("invoice_fact", context)
