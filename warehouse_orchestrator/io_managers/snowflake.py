"""
WarehouseSnowflakeIOManager — context-driven Snowflake writer.

handle_output
    Receives a compiled SQL SELECT string from an asset, creates a Snowflake
    TEMPORARY TABLE from it, then writes to the target table using one of three
    strategies chosen by the partition key prefix:

    Strategy        Partition key prefix    Use case
    ──────────────  ──────────────────────  ──────────────────────────────────────
    MERGE           incr: / daily: / range: Handle C (insert) + U (update) ops.
                                            Outer WHERE narrows the temp table to
                                            the date range before merging.
    DELETE+INSERT   store: / org:           Full scope rebuild.
                                            Delete all rows for the scope, then
                                            re-insert from the compiled SQL.
    TRUNCATE+INSERT full                    Monthly reset.
                                            Clears D (deleted) rows and any drift.

load_input
    Reads all rows from the upstream Snowflake table into a pandas DataFrame.
    Validates against the registered Pydantic schema (if any).

Data flow
    Asset returns compiled SQL (str)
        → IOManager creates TEMP TABLE from compiled SQL (+ outer WHERE if needed)
        → IOManager writes to permanent Snowflake table
        → data never leaves Snowflake into Python memory during writes

Schema validation (load_input only)
    Register Pydantic models via register_schema().  Validation runs when reading
    data back into Python — not on the write path (data stays in Snowflake).
"""

import logging
import os
import time
from typing import Any, Optional, Type

import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import BaseModel

from warehouse_orchestrator.partitions import PartitionScope, parse_partition_key

logger = logging.getLogger(__name__)


class WarehouseSnowflakeIOManager(ConfigurableIOManager):
    """
    Partition-aware Snowflake IOManager.

    Configuration:
        database:  Target Snowflake database (e.g. NITIN_DAGSTER_POC).
    """

    database: str

    # Pydantic schema map registered at startup — used only in load_input.
    # Not a Pydantic field (Type objects can't be serialised); managed as a
    # plain instance attribute.
    _schema_map: dict[str, Type[BaseModel]] = {}

    def register_schema(self, model_name: str, schema_cls: Type[BaseModel]) -> None:
        self._schema_map[model_name.lower()] = schema_cls

    # ------------------------------------------------------------------ #
    # Snowflake connection                                                #
    # ------------------------------------------------------------------ #

    def _get_connection(self):
        import snowflake.connector
        from cryptography.hazmat.primitives.serialization import (
            load_pem_private_key,
            Encoding,
            PrivateFormat,
            NoEncryption,
        )

        account   = os.environ["SNOWFLAKE_ACCOUNT_NAME"]
        user      = os.environ.get("SNOWFLAKE_USER_NAME", "nitin_dagster_service")
        role      = os.environ.get("SNOWFLAKE_ROLE", "NITIN_DAGSTER_ROLE")
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_DEV")
        key_path  = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "/Users/nitin/hackathon_key.p8")
        passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

        logger.info(
            "Snowflake connect | account=%s  user=%s  role=%s  warehouse=%s  database=%s  key=%s",
            account, user, role, warehouse, self.database, key_path,
        )

        with open(key_path, "rb") as f:
            private_key = load_pem_private_key(
                f.read(),
                password=passphrase.encode() if passphrase else None,
            )

        private_key_bytes = private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )

        return snowflake.connector.connect(
            account=account,
            user=user,
            private_key=private_key_bytes,
            authenticator="snowflake_jwt",
            role=role,
            warehouse=warehouse,
            database=self.database,
            schema="PUBLIC",  # session default for TEMP TABLE creation
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _resolve_table(self, key_path: list[str]) -> tuple[str, str]:
        """
        Asset key path: ["warehouse", "bronze", "base_pos_ticket"]
        Returns: ("BRONZE", "BASE_DISPENSARY_POSPER_TICKET")
        """
        if len(key_path) >= 2:
            return key_path[-2].upper(), key_path[-1].upper()
        return "PUBLIC", key_path[-1].upper()

    def _get_columns(self, cur, tmp_name: str) -> list[str]:
        """Return column names from a (temp) table, preserving order."""
        cur.execute(f"DESCRIBE TABLE {tmp_name}")
        return [row[0] for row in cur.fetchall()]

    def _build_temp_select(
        self,
        compiled_sql: str,
        scope: PartitionScope,
        partition_expr: Optional[str],
        schema: str,
    ) -> str:
        """
        Wrap compiled SQL with an outer WHERE clause so the temp table only
        contains the rows relevant to this partition run.

        Bronze models have all scope baked in at compile time (date/store vars for
        range:/store: runs, or load_date two-phase CTE for incr:/daily:/full runs).
        The outer WHERE must NOT be applied to bronze — it would incorrectly filter
        out late-arriving updates whose process_date falls outside the partition window.

        Exception: org-scoped delete_insert on bronze.  The bronze SQL has no org_id
        var (store_ids for the org are not known at compile time), so without an outer
        filter the temp table becomes a full scan of the entire POS_STAGING staging table.
        Wrap with the same store_id subquery used in the DELETE step so the temp table
        contains only rows for the org's stores.

        Silver/gold models have no var blocks; the outer WHERE is essential for them.
        """
        # Bronze handles its own scope — outer WHERE would break late-arriving updates.
        # Exception: org-scoped delete_insert (see docstring above).
        if schema == "BRONZE":
            if scope.strategy == "delete_insert" and scope.org_id:
                return (
                    f"SELECT * FROM (\n{compiled_sql}\n) AS src\n"
                    f"WHERE src.store_id IN ("
                    f"SELECT store_id FROM {self.database}.BRONZE.TENANT_STORES "
                    f"WHERE org_id = '{scope.org_id}')"
                )
            return compiled_sql

        where_parts: list[str] = []

        if scope.strategy == "merge" and partition_expr and scope.has_date_range():
            where_parts.append(
                f"src.{partition_expr}::DATE BETWEEN '{scope.date_from}' AND '{scope.date_to}'"
            )

        elif scope.strategy == "delete_insert":
            if scope.store_id:
                where_parts.append(f"src.store_id = '{scope.store_id}'")
            elif scope.org_id:
                # Use store_id IN (subquery) for all layers.  Not all silver/gold
                # models expose org_id (e.g. customer_first_tx_timestamp only has
                # store_id + customer_id), so filtering by src.org_id would fail.
                # Every model has store_id, and TENANT_STORES is a small lookup.
                where_parts.append(
                    f"src.store_id IN ("
                    f"SELECT store_id FROM {self.database}.BRONZE.TENANT_STORES "
                    f"WHERE org_id = '{scope.org_id}')"
                )

        if where_parts:
            return f"SELECT * FROM (\n{compiled_sql}\n) AS src\nWHERE {' AND '.join(where_parts)}"
        return compiled_sql

    # ------------------------------------------------------------------ #
    # Write strategies                                                    #
    # ------------------------------------------------------------------ #

    def _merge(
        self,
        cur,
        schema: str,
        table: str,
        tmp_name: str,
        columns: list[str],
        unique_keys: list[str],
    ) -> None:
        target = f"{self.database}.{schema}.{table}"

        join_cond = " AND ".join(f"tgt.{c} = src.{c}" for c in unique_keys)
        update_set = ", ".join(f"tgt.{c} = src.{c}" for c in columns)
        col_list = ", ".join(columns)
        val_list = ", ".join(f"src.{c}" for c in columns)

        sql = f"""
MERGE INTO {target} AS tgt
USING {tmp_name} AS src
ON {join_cond}
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
"""
        cur.execute(sql)
        logger.info(
            "MERGE → %s.%s | rows_affected=%s | matched_on=%s",
            schema, table, cur.rowcount, unique_keys,
        )

    def _delete_insert(
        self,
        cur,
        schema: str,
        table: str,
        tmp_name: str,
        scope: PartitionScope,
    ) -> None:
        target = f"{self.database}.{schema}.{table}"

        if scope.store_id:
            cur.execute(f"DELETE FROM {target} WHERE store_id = '{scope.store_id}'")
        elif scope.org_id:
            # Use store_id IN (subquery) for all layers — same reason as
            # _build_temp_select: not all silver/gold models have org_id.
            cur.execute(
                f"DELETE FROM {target} WHERE store_id IN ("
                f"SELECT store_id FROM {self.database}.BRONZE.TENANT_STORES "
                f"WHERE org_id = '{scope.org_id}')"
            )
        logger.info("DELETE → %s.%s | rows_removed=%s", schema, table, cur.rowcount)

        cur.execute(f"INSERT INTO {target} SELECT * FROM {tmp_name}")
        logger.info("INSERT → %s.%s | rows_inserted=%s", schema, table, cur.rowcount)

    def _truncate_insert(
        self,
        cur,
        schema: str,
        table: str,
        tmp_name: str,
    ) -> None:
        target = f"{self.database}.{schema}.{table}"
        cur.execute(f"TRUNCATE TABLE {target}")
        logger.info("TRUNCATE → %s.%s", schema, table)
        cur.execute(f"INSERT INTO {target} SELECT * FROM {tmp_name}")
        logger.info("INSERT → %s.%s | rows_inserted=%s", schema, table, cur.rowcount)

    # ------------------------------------------------------------------ #
    # IOManager interface                                                 #
    # ------------------------------------------------------------------ #

    def handle_output(self, context: OutputContext, obj: Optional[str]) -> None:
        """
        Write compiled SQL output to Snowflake.

        Args:
            obj: Plain SQL SELECT string produced by an asset.
                 None means the asset managed its own write — this becomes a no-op.
        """
        if obj is None:
            context.log.info("handle_output received None — asset managed its own write, skipping.")
            return

        schema, table = self._resolve_table(list(context.asset_key.path))
        scope = parse_partition_key(context.partition_key)

        # Bronze assets never do a full POS_STAGING rebuild.
        # For full partition: use MERGE (load_date two-phase incremental).
        # Silver/gold keep truncate_insert on full partition (rebuild from bronze).
        asset_layer = list(context.asset_key.path)[1] if len(context.asset_key.path) >= 2 else ""
        if scope.strategy == "truncate_insert" and asset_layer == "bronze":
            scope = PartitionScope(strategy="merge")
            context.log.info(
                "Bronze asset on 'full' partition — overriding strategy to MERGE "
                "(bronze always uses load_date incremental, never a full POS_STAGING rebuild)"
            )

        meta = context.definition_metadata or {}
        partition_expr: Optional[str] = meta.get("partition_expr")
        unique_keys: list[str] = list(meta.get("unique_keys", []))

        context.log.debug("Compiled SQL:\n%s", obj)

        context.log.info(
            "handle_output starting | table=%s.%s | partition=%s | strategy=%s | "
            "date_from=%s | date_to=%s | store_id=%s | org_id=%s | "
            "partition_expr=%s | unique_keys=%s",
            schema, table,
            context.partition_key,
            scope.strategy,
            scope.date_from, scope.date_to,
            scope.store_id, scope.org_id,
            partition_expr, unique_keys,
        )

        tmp_name = f"_tmp_{table.lower()}"
        t0 = time.time()

        conn = self._get_connection()
        try:
            cur = conn.cursor()

            # ------------------------------------------------------------------ #
            # TRUNCATE+INSERT fast path — skip temp table when table exists       #
            # For MERGE / DELETE+INSERT the temp table is necessary (atomic       #
            # staging before the write). For TRUNCATE+INSERT we are wiping the    #
            # target anyway, so staging into a temp first just doubles the I/O.   #
            # ------------------------------------------------------------------ #
            if scope.strategy == "truncate_insert":
                target = f"{self.database}.{schema}.{table}"

                # DDL bootstrap — create schema/table if this is the first run.
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{schema}")
                cur.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {self.database}.{schema}")
                table_exists = bool(cur.fetchall())

                if not table_exists:
                    # First run: need temp table to derive column structure.
                    context.log.info(
                        "TRUNCATE+INSERT: new table — bootstrapping DDL from temp table for %s.%s",
                        schema, table,
                    )
                    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp_name} AS\n{obj}")
                    cur.execute(f"CREATE TABLE {target} LIKE {tmp_name}")
                    context.log.info("Created table %s.%s", schema, table)
                    cur.execute(f"INSERT INTO {target} SELECT * FROM {tmp_name}")
                    context.log.info(
                        "INSERT → %s.%s | rows_inserted=%s", schema, table, cur.rowcount,
                    )
                else:
                    # Table exists: direct TRUNCATE + INSERT, no temp table overhead.
                    context.log.info(
                        "TRUNCATE+INSERT: direct write (no temp table) → %s.%s", schema, table,
                    )
                    cur.execute(f"TRUNCATE TABLE {target}")
                    context.log.info("TRUNCATE done → %s.%s", schema, table)
                    cur.execute(f"INSERT INTO {target}\n{obj}")
                    context.log.info(
                        "INSERT → %s.%s | rows_inserted=%s", schema, table, cur.rowcount,
                    )

                conn.commit()
                elapsed = time.time() - t0
                context.log.info(
                    "Write complete → %s.%s | strategy=truncate_insert | elapsed=%.2fs",
                    schema, table, elapsed,
                )
                return

            # ------------------------------------------------------------------ #
            # MERGE / DELETE+INSERT — build temp table first, then write          #
            # ------------------------------------------------------------------ #

            # Build temp-table SELECT (with outer WHERE if needed)
            temp_select = self._build_temp_select(obj, scope, partition_expr, schema)
            outer_where_applied = temp_select != obj
            context.log.info(
                "Outer WHERE applied: %s%s",
                outer_where_applied,
                f" (filtering on {partition_expr})" if outer_where_applied and partition_expr else
                " (store/org scope)" if outer_where_applied else
                " — customer_first_tx_timestamp style full-scan" if not partition_expr else "",
            )

            # ------------------------------------------------------------------ #
            # Pre-flight DDL check (MERGE path only)                             #
            #                                                                     #
            # Bronze incremental SQL references {{ this }} (the target table) in  #
            # the incremental threshold subquery, e.g.:                           #
            #   SELECT COALESCE(MAX(load_date), '2000-01-01') FROM {{ this }}     #
            #                                                                     #
            # Snowflake validates ALL table references at SQL compile time, so    #
            # the CREATE TEMP TABLE fails with "does not exist" on first run even #
            # though COALESCE would handle the NULL at runtime.                   #
            #                                                                     #
            # Fix: check table existence BEFORE the temp table creation.          #
            # If the target doesn't exist yet, replace the rendered {{ this }}    #
            # reference with a zero-row inline subquery so COALESCE resolves to   #
            # '2000-01-01' and a full initial load is performed.                  #
            # After the temp table is created, bootstrap the real target table    #
            # using CREATE TABLE ... LIKE tmp (preserves column types/order).     #
            # ------------------------------------------------------------------ #
            target_exists = True
            target_full = f"{self.database}.{schema}.{table}"

            # Pre-flight: check target existence for MERGE and DELETE+INSERT.
            # MERGE also rewrites {{ this }} references with an empty subquery on
            # first run (bronze incremental SQL references the target in the
            # load_date CTE).  DELETE+INSERT only needs the existence check.
            if scope.strategy in ("merge", "delete_insert"):
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{schema}")
                cur.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {self.database}.{schema}")
                target_exists = bool(cur.fetchall())

                if not target_exists and scope.strategy == "merge":
                    # Replace rendered {{ this }} references with an empty inline table.
                    # MAX() on an empty result set returns NULL; COALESCE handles it.
                    # Both load_date (POS_STAGING-based models) and sync_date
                    # (tenant_stores) are included so all bronze models are covered.
                    target_ref = f'"{self.database}"."{schema}"."{table}"'
                    empty_ref = (
                        "(SELECT NULL::TIMESTAMP_NTZ AS load_date, "
                        "NULL::TIMESTAMP_NTZ AS sync_date WHERE FALSE) AS _empty_target"
                    )
                    temp_select = temp_select.replace(f"from {target_ref}", f"from {empty_ref}")
                    temp_select = temp_select.replace(f"FROM {target_ref}", f"FROM {empty_ref}")
                    context.log.info(
                        "First run for %s.%s — replaced '{{ this }}' references with "
                        "empty subquery; initial full load will run via COALESCE epoch floor.",
                        schema, table,
                    )

            # Create temp table
            context.log.info("Creating temp table: %s", tmp_name)
            cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp_name} AS\n{temp_select}")

            # Accurate row count from temp table
            cur.execute(f"SELECT COUNT(*) FROM {tmp_name}")
            tmp_row_count = cur.fetchone()[0]

            # Column list from temp table (preserves Snowflake column order)
            columns = self._get_columns(cur, tmp_name)
            context.log.info(
                "Temp table ready: %s | rows=%d | columns=%d (%s)",
                tmp_name, tmp_row_count, len(columns), ", ".join(columns),
            )

            # DDL bootstrap: create target table from temp schema if this is the
            # first run.  Runs BEFORE the empty-table early exit so the table
            # always exists after bronze runs — even when the partition scope
            # produces 0 rows (e.g. a store: test run after manually dropping a
            # bronze table that has no staging data for that store yet).
            if not target_exists:
                context.log.info(
                    "DDL bootstrap: creating new table %s.%s from temp table schema",
                    schema, table,
                )
                try:
                    cur.execute(f"CREATE TABLE {target_full} LIKE {tmp_name}")
                    context.log.info("Created table %s.%s", schema, table)
                except Exception as e:  # noqa: BLE001
                    # Snowflake error 42710 / 003041: table already exists but
                    # SHOW TABLES returned nothing because the current role lacks
                    # MONITOR privilege on it.  The table is present — treat it
                    # as such and let the subsequent MERGE/DELETE+INSERT proceed.
                    # If that also fails for privilege reasons, the error will
                    # surface there with full context.
                    err_str = str(e)
                    if "already exists" in err_str and "no privileges" in err_str:
                        context.log.warning(
                            "Table %s already exists but was not visible via SHOW TABLES "
                            "(current role has no MONITOR privilege on it). "
                            "Treating as existing table and continuing. "
                            "Grant MONITOR + SELECT/INSERT/UPDATE/DELETE on the table "
                            "to the current role to avoid this warning.",
                            target_full,
                        )
                        target_exists = True
                    else:
                        raise

            if tmp_row_count == 0:
                context.log.warning(
                    "Temp table %s is empty — no rows matched the partition scope. "
                    "Skipping write to avoid no-op %s.",
                    tmp_name, scope.strategy.upper(),
                )
                return

            # Execute write strategy
            if scope.strategy == "merge":
                if not unique_keys:
                    raise ValueError(
                        f"No unique_keys declared in asset metadata for {schema}.{table}. "
                        "MERGE requires unique_keys."
                    )
                context.log.info("Executing MERGE → %s.%s on keys=%s", schema, table, unique_keys)
                self._merge(cur, schema, table, tmp_name, columns, unique_keys)

            elif scope.strategy == "delete_insert":
                context.log.info(
                    "Executing DELETE+INSERT → %s.%s | scope: store_id=%s  org_id=%s",
                    schema, table, scope.store_id, scope.org_id,
                )
                self._delete_insert(cur, schema, table, tmp_name, scope)

            conn.commit()
            elapsed = time.time() - t0
            context.log.info(
                "Write complete → %s.%s | strategy=%s | elapsed=%.2fs",
                schema, table, scope.strategy, elapsed,
            )

        finally:
            conn.close()

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """
        Read the upstream Snowflake table into a pandas DataFrame.
        Validates against the registered Pydantic schema if one is registered.
        """
        schema, table = self._resolve_table(list(context.asset_key.path))
        sql = f"SELECT * FROM {self.database}.{schema}.{table}"

        context.log.info("load_input: reading %s.%s.%s into DataFrame", self.database, schema, table)
        t0 = time.time()
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [desc[0].lower() for desc in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()

        elapsed = time.time() - t0
        context.log.info(
            "load_input complete | %s.%s | rows=%d | columns=%d | elapsed=%.2fs",
            schema, table, len(df), len(df.columns), elapsed,
        )

        schema_cls = self._schema_map.get(table.lower())
        if schema_cls is not None:
            context.log.info("Validating %s.%s against Pydantic schema %s", schema, table, schema_cls.__name__)
            self._validate(df, schema_cls, f"{schema}.{table}")
            context.log.info("Schema validation passed for %s.%s", schema, table)
        else:
            context.log.debug("No Pydantic schema registered for %s.%s — skipping validation", schema, table)

        return df

    def _validate(
        self,
        df: pd.DataFrame,
        schema_cls: Type[BaseModel],
        label: str,
    ) -> None:
        errors: list[str] = []
        for i, row in enumerate(df.to_dict("records")):
            try:
                schema_cls(**row)
            except Exception as exc:
                errors.append(f"row {i}: {exc}")
                if len(errors) >= 5:
                    errors.append("… (truncated)")
                    break
        if errors:
            raise ValueError(
                f"Schema validation failed for {label}:\n" + "\n".join(errors)
            )
