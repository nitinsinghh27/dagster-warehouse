"""
Snowflake resource factory.

Reads all credentials from environment variables (loaded via .envrc / dotenv).
Returns a configured dagster-snowflake SnowflakeResource ready for use in
assets and jobs that need a direct Snowflake connection (e.g. manual queries,
schema inspection, or administrative tasks).

The IOManager has its own internal connection logic (_get_connection) so it
doesn't depend on this resource — but assets that need to run ad-hoc SQL
can inject SnowflakeResource directly.
"""

from __future__ import annotations

import os

from dagster_snowflake import SnowflakeResource


def build_snowflake_resource() -> SnowflakeResource:
    """Construct a SnowflakeResource from environment variables."""
    return SnowflakeResource(
        account=os.environ.get("SNOWFLAKE_ACCOUNT_NAME", ""),
        user=os.environ.get("SNOWFLAKE_USER_NAME", ""),
        password=os.environ.get("SNOWFLAKE_USER_PASSWORD", ""),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "username_password_mfa"),
        role=os.environ.get("SNOWFLAKE_ROLE", "DATA_ENG"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_DEV"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "NITIN_DAGSTER_POC"),
    )
