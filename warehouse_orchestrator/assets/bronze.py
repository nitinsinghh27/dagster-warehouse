"""
Bronze layer assets — one @asset per table.

Each asset:
  1. Parses the partition key to build dbt render vars.
  2. Renders the dbt model SQL via the Python Jinja compiler
     (is_incremental() = False; date/store vars baked in for bronze).
  3. Returns the compiled SQL string to the IOManager.

The IOManager (WarehouseSnowflakeIOManager) receives the SQL, wraps it in a
Snowflake TEMPORARY TABLE (with an additional outer WHERE for silver/gold),
then executes MERGE / DELETE+INSERT / TRUNCATE+INSERT depending on the
partition key strategy.

Bronze partition_expr is "process_date" for all CDC tables.
tenant_stores is a store-dimension table — same process_date filter applies.
"""

import os
from pathlib import Path

import dagster as dg

from warehouse_orchestrator.compiler import compile_model
from warehouse_orchestrator.partitions import scope_to_bronze_vars, warehouse_partitions, is_incremental_partition

_DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"


def _compile(model_name: str, partition_key: str, context: dg.AssetExecutionContext) -> str:
    """Render a bronze dbt model to plain SQL for the given partition."""
    render_vars = scope_to_bronze_vars(partition_key)
    use_incremental = is_incremental_partition(partition_key)
    context.log.info(
        "[bronze] %s | partition=%s | is_incremental=%s | render_vars=%s",
        model_name, partition_key, use_incremental, render_vars,
    )
    return compile_model(
        model_name=model_name,
        layer="bronze",
        dbt_project_dir=_DBT_PROJECT_DIR,
        render_vars=render_vars,
        database=os.environ.get("SNOWFLAKE_DATABASE", "NITIN_DAGSTER_POC"),
        is_incremental=use_incremental,
    )


# ---------------------------------------------------------------------------
# base_pos_ticket
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "bronze"],
    partitions_def=warehouse_partitions,
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticket_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="bronze",
    description="POS ticket headers — one row per CDC-deduplicated transaction.",
)
def base_pos_ticket(context: dg.AssetExecutionContext) -> str:
    return _compile("base_pos_ticket", context.partition_key, context)


# ---------------------------------------------------------------------------
# base_pos_ticketline
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "bronze"],
    partitions_def=warehouse_partitions,
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "ticketline_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="bronze",
    description="POS line items — one row per ticket line after CDC deduplication.",
)
def base_pos_ticketline(context: dg.AssetExecutionContext) -> str:
    return _compile("base_pos_ticketline", context.partition_key, context)


# ---------------------------------------------------------------------------
# base_pos_ticketline_tax_log
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "bronze"],
    partitions_def=warehouse_partitions,
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "tax_log_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="bronze",
    description="Per-line tax events from CRM area tax log.",
)
def base_pos_ticketline_tax_log(context: dg.AssetExecutionContext) -> str:
    return _compile(
        "base_pos_ticketline_tax_log", context.partition_key, context
    )


# ---------------------------------------------------------------------------
# base_inventory
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "bronze"],
    partitions_def=warehouse_partitions,
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "inventory_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="bronze",
    description="Inventory batches with cost data.",
)
def base_inventory(context: dg.AssetExecutionContext) -> str:
    return _compile("base_inventory", context.partition_key, context)


# ---------------------------------------------------------------------------
# base_invoice_line_cost
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "bronze"],
    partitions_def=warehouse_partitions,
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id", "invoice_line_cost_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="bronze",
    description="Per-line COGS breakdown from invoice line cost.",
)
def base_invoice_line_cost(context: dg.AssetExecutionContext) -> str:
    return _compile("base_invoice_line_cost", context.partition_key, context)


# ---------------------------------------------------------------------------
# tenant_stores
# ---------------------------------------------------------------------------

@dg.asset(
    key_prefix=["warehouse", "bronze"],
    partitions_def=warehouse_partitions,
    metadata={
        "partition_expr": "process_date",
        "unique_keys": ["store_id"],
    },
    io_manager_key="warehouse_io_manager",
    group_name="bronze",
    description="Store/tenant dimension — org, store metadata, analytics IDs.",
)
def tenant_stores(context: dg.AssetExecutionContext) -> str:
    return _compile("tenant_stores", context.partition_key, context)
