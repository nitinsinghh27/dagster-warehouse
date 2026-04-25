"""
Pydantic schemas for the BRONZE layer.

These define the expected shape of each bronze model's output.
Used by:
  - SnowflakeTableIOManager for handle_input / handle_output validation
  - Downstream Python assets that read bronze tables

All fields are Optional except the primary / partition keys to reflect
the reality of nullable CDC source data.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class BronzeTicket(BaseModel):
    """base_pos_ticket"""

    store_id: str
    ticket_id: str
    id: Optional[str] = None
    date_close: Optional[datetime] = None
    date_open: Optional[datetime] = None
    ticket_type: Optional[str] = None
    revenue_source: Optional[str] = None
    ticket_source: Optional[str] = None
    customer_type: Optional[str] = None
    customer_id: Optional[str] = None
    employee_id: Optional[str] = None
    subtotal: Optional[float] = None
    total_price: Optional[float] = None
    discount_amount: Optional[float] = None
    return_amount: Optional[float] = None
    tier_discount_amount: Optional[float] = None
    tax_total: Optional[float] = None
    gross_receipts: Optional[float] = None
    sync_date: Optional[datetime] = None
    process_date: Optional[datetime] = None
    load_date: Optional[datetime] = None


class BronzeTicketLine(BaseModel):
    """base_pos_ticketline"""

    store_id: str
    ticketline_id: str
    ticket_id: Optional[str] = None
    product_id: Optional[str] = None
    inventory_id: Optional[str] = None
    batch_id: Optional[str] = None
    quantity: Optional[float] = None
    unit_price: Optional[float] = None
    line_total: Optional[float] = None
    discount_amount: Optional[float] = None
    return_amount: Optional[float] = None
    description: Optional[str] = None
    sync_date: Optional[datetime] = None
    process_date: Optional[datetime] = None
    load_date: Optional[datetime] = None


class BronzeTaxLog(BaseModel):
    """base_pos_ticketline_tax_log"""

    store_id: str
    tax_log_id: str
    ticket_id: Optional[str] = None
    ticketline_id: Optional[str] = None
    tax_amount: Optional[float] = None
    excise_tax_amount: Optional[float] = None
    sales_tax_amount: Optional[float] = None
    sync_date: Optional[datetime] = None
    process_date: Optional[datetime] = None
    load_date: Optional[datetime] = None


class BronzeInventory(BaseModel):
    """base_inventory"""

    store_id: str
    inventory_id: str
    product_id: Optional[str] = None
    batch_id: Optional[str] = None
    excise_cost: Optional[float] = None
    non_excise_cost: Optional[float] = None
    sync_date: Optional[datetime] = None
    process_date: Optional[datetime] = None
    load_date: Optional[datetime] = None


class BronzeInvoiceLineCost(BaseModel):
    """base_invoice_line_cost"""

    store_id: str
    invoice_line_cost_id: str
    product_id: Optional[str] = None
    batch_id: Optional[str] = None
    cost_with_excise: Optional[float] = None
    cost_without_excise: Optional[float] = None
    true_cost_without_excise: Optional[float] = None
    base_cost: Optional[float] = None
    sync_date: Optional[datetime] = None
    process_date: Optional[datetime] = None
    load_date: Optional[datetime] = None


class BronzeTenant(BaseModel):
    """tenant_stores"""

    store_id: str
    org_id: Optional[str] = None
    org_name: Optional[str] = None
    store_name: Optional[str] = None
    store_url: Optional[str] = None
    external_store_id: Optional[str] = None
    analytics_id: Optional[str] = None
    sync_date: Optional[datetime] = None
    process_date: Optional[datetime] = None


# Registry: lower-cased dbt model name → Pydantic class
BRONZE_SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "base_pos_ticket":              BronzeTicket,
    "base_pos_ticketline":          BronzeTicketLine,
    "base_pos_ticketline_tax_log": BronzeTaxLog,
    "base_inventory":               BronzeInventory,
    "base_invoice_line_cost":        BronzeInvoiceLineCost,
    "tenant_stores":                              BronzeTenant,
}
