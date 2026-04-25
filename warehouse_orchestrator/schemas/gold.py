"""
Pydantic schemas for the GOLD layer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, field_validator


class GoldHourlySales(BaseModel):
    """ra_hourly_sales"""

    synth_key: str

    # Identifiers / dimensions
    org_id: str
    store_id: str
    analytics_id: str
    org_name: Optional[str] = None
    store_name: Optional[str] = None
    store_url: Optional[str] = None
    external_store_id: Optional[str] = None

    # Time grain
    hour_close: datetime

    # Categorical dimensions
    ticket_type: str
    ticket_source: Optional[str] = None
    revenue_source: str
    customer_type: str
    customer_visit_type: Literal["new", "returning", "walk_in"]
    is_sample: bool
    is_promo: bool

    # Counts
    customer_count: int
    ticket_count: int

    # Volume
    units_sold: Optional[float] = None

    # Revenue
    gross_sales: float
    net_sales: float
    discounts: Optional[float] = None
    returns: Optional[float] = None
    tier_discounts: Optional[float] = None
    gross_receipts: Optional[float] = None

    # Taxes
    taxes: Optional[float] = None

    # COGS
    cost_with_excise: Optional[float] = None
    cost_without_excise: Optional[float] = None
    true_cost_without_excise: Optional[float] = None
    base_cost: Optional[float] = None
    sample_cost_with_excise: Optional[float] = None
    promo_cost_with_excise: Optional[float] = None

    # Freshness
    last_sync: Optional[datetime] = None

    @field_validator("customer_count", "ticket_count")
    @classmethod
    def must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("count must be >= 0")
        return v


# Registry
GOLD_SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "ra_hourly_sales": GoldHourlySales,
}
