"""
Partition definitions and scope utilities for the warehouse pipeline.

Partition key convention
------------------------
"incr:YYYY-MM-DD"       3-hourly incremental  — MERGE last 2 days from anchor date
"daily:YYYY-MM-DD"      Daily rolling refresh — MERGE last 30 days from anchor date
"range:YYYY-MM-DD:N"    Custom range          — MERGE last N days from anchor date
"full"                  Monthly full rebuild  — TRUNCATE + INSERT (clears deleted rows)
                        Bronze exception: uses MERGE (load_date two-phase incremental)
"store:STORE_ID"        Store scope           — DELETE WHERE store_id + INSERT
"org:ORG_ID"            Org scope             — DELETE WHERE org_id + INSERT

Key helpers
-----------
is_incremental_partition(key)  True for incr:, daily:, and full — bronze uses the
                               two-phase load_date MERGE pattern for these.
scope_to_bronze_vars(key)      Build dbt --vars dict for bronze model rendering.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from dagster import DynamicPartitionsDefinition

warehouse_partitions = DynamicPartitionsDefinition(name="warehouse")


@dataclass
class PartitionScope:
    """Parsed representation of a warehouse partition key."""

    strategy: str            # "merge" | "delete_insert" | "truncate_insert"
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    store_id: Optional[str] = None
    org_id: Optional[str] = None

    def has_date_range(self) -> bool:
        return self.date_from is not None and self.date_to is not None


def parse_partition_key(key: str) -> PartitionScope:
    """Convert a warehouse partition key string into a PartitionScope."""
    if key == "full":
        return PartitionScope(strategy="truncate_insert")

    parts = key.split(":", 1)
    prefix = parts[0]
    rest = parts[1] if len(parts) > 1 else ""

    if prefix == "incr":
        anchor = date.fromisoformat(rest)
        return PartitionScope(
            strategy="merge",
            date_from=anchor - timedelta(days=2),
            date_to=anchor,
        )

    if prefix == "daily":
        anchor = date.fromisoformat(rest)
        return PartitionScope(
            strategy="merge",
            date_from=anchor - timedelta(days=30),
            date_to=anchor,
        )

    if prefix == "range":
        anchor_str, days_str = rest.split(":", 1)
        anchor = date.fromisoformat(anchor_str)
        return PartitionScope(
            strategy="merge",
            date_from=anchor - timedelta(days=int(days_str)),
            date_to=anchor,
        )

    if prefix == "store":
        return PartitionScope(strategy="delete_insert", store_id=rest)

    if prefix == "org":
        return PartitionScope(strategy="delete_insert", org_id=rest)

    raise ValueError(f"Unknown partition key format: {key!r}")


def is_incremental_partition(key: str) -> bool:
    """
    True when the bronze SQL should use the two-phase load_date incremental pattern.

    Covers incr:, daily:, and full — all cases where bronze uses MERGE (not a
    date-windowed or store/org scoped scan).  range:, store:, org: are excluded
    because they use explicit var-based filters instead.
    """
    if key == "full":
        return True
    return key.startswith("incr:") or key.startswith("daily:")


def scope_to_bronze_vars(key: str) -> dict:
    """
    Build the dbt --vars dict for bronze model rendering.

    For incr:, daily:, and full partition keys, bronze SQL uses the two-phase
    load_date incremental pattern (records_to_sync CTE + EXISTS).  No date vars
    are needed — scope is handled entirely inside the SQL via MAX(load_date).

    For range:, store:, org: — explicit var-based filters are still used.

    Silver/gold models have no var filters; they rely on the IOManager outer WHERE.
    """
    base: dict = {"python_controlled": True}

    # incr/daily/full — scope handled by load_date two-phase SQL, not date vars
    if is_incremental_partition(key):
        return base

    if key == "full":  # already handled above, but kept for clarity
        return base

    scope = parse_partition_key(key)

    if scope.date_from:
        base["date_from"] = str(scope.date_from)
    if scope.date_to:
        base["date_to"] = str(scope.date_to)
    if scope.store_id:
        base["store_ids"] = [scope.store_id]
    # org_id is not a supported bronze var — IOManager handles org scope via subquery

    return base
