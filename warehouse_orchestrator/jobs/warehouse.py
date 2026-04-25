"""
Warehouse pipeline jobs and schedules.

Three jobs — same asset selection, same partition definition, different schedules
and partition key prefixes. The IOManager reads the prefix to choose the write strategy.

Job              Schedule                    Partition key      IOManager strategy
───────────────  ──────────────────────────  ─────────────────  ──────────────────────
incremental_job  Every 3h, 03:15–21:15 LA   incr:YYYY-MM-DD    MERGE last 2 days
weekly_job       Sunday 01:15 LA            daily:YYYY-MM-DD   MERGE last 30 days
monthly_job      1st of month 02:00 LA      full               TRUNCATE + INSERT

Manual partition keys (type in UI → Add a partition):
  range:YYYY-MM-DD:N   MERGE last N days from anchor date
  store:STORE_ID       DELETE + INSERT for one store
  org:ORG_ID           DELETE + INSERT for one org (bronze resolved via tenant subquery)

Jobs are kept separate so Dagster UI run history is filterable by job name.
"""

from datetime import date

from dagster import (
    AssetSelection,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from warehouse_orchestrator.partitions import warehouse_partitions

# ---------------------------------------------------------------------------
# Job definitions — identical structure, different names for UI observability
# ---------------------------------------------------------------------------

incremental_job = define_asset_job(
    name="incremental_job",
    selection=AssetSelection.all(),
    partitions_def=warehouse_partitions,
    description=(
        "3-hourly incremental run — MERGE last 2 days. "
        "Handles Debezium C (create) and U (update) ops. "
        "Idempotent: re-running covers the same 2-day window without duplicates."
    ),
)

weekly_refresh_job = define_asset_job(
    name="weekly_refresh_job",
    selection=AssetSelection.all(),
    partitions_def=warehouse_partitions,
    description=(
        "Weekly rolling refresh — MERGE last 30 days. "
        "Catches late-arriving U ops (e.g. price corrections weeks after the source event). "
        "Aggregated values may decrease on past dates — this is correct (returns, voids)."
    ),
)

monthly_full_job = define_asset_job(
    name="monthly_full_job",
    selection=AssetSelection.all(),
    partitions_def=warehouse_partitions,
    description=(
        "Monthly full rebuild — TRUNCATE + INSERT all data. "
        "Clears Debezium D (delete) row drift that MERGE cannot remove. "
        "Also bootstraps schema/table DDL for any newly added assets."
    ),
)

# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

@schedule(
    job=incremental_job,
    cron_schedule="15 3-21/3 * * *",   # 03:15, 06:15, 09:15, 12:15, 15:15, 18:15, 21:15 LA
    execution_timezone="America/Los_Angeles",
)
def incremental_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    today = date.today().isoformat()
    partition_key = f"incr:{today}"
    context.instance.add_dynamic_partitions(
        partitions_def_name="warehouse",
        partition_keys=[partition_key],
    )
    return RunRequest(partition_key=partition_key)


@schedule(
    job=weekly_refresh_job,
    cron_schedule="15 1 * * 0",         # Sunday 01:15 LA
    execution_timezone="America/Los_Angeles",
)
def weekly_refresh_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    today = date.today().isoformat()
    partition_key = f"daily:{today}"
    context.instance.add_dynamic_partitions(
        partitions_def_name="warehouse",
        partition_keys=[partition_key],
    )
    return RunRequest(partition_key=partition_key)


@schedule(
    job=monthly_full_job,
    cron_schedule="0 2 1 * *",          # 1st of month 02:00 LA
    execution_timezone="America/Los_Angeles",
)
def monthly_full_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    partition_key = "full"
    context.instance.add_dynamic_partitions(
        partitions_def_name="warehouse",
        partition_keys=[partition_key],
    )
    return RunRequest(partition_key=partition_key)
