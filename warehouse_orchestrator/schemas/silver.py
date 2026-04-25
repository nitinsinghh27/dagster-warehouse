"""
Pydantic schemas for the SILVER layer.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel


class SilverTicketLineSales(BaseModel):
    """ticketline_sales"""

    store_id: str
    ticketline_id: str
    ticket_id: Optional[str] = None
    org_id: Optional[str] = None
    org_name: Optional[str] = None
    store_name: Optional[str] = None
    store_url: Optional[str] = None
    external_store_id: Optional[str] = None
    analytics_id: Optional[str] = None
    date_close: Optional[datetime] = None
    date_open: Optional[datetime] = None
    ticket_type: Optional[str] = None
    revenue_source: Optional[str] = None
    ticket_source: Optional[str] = None
    customer_type: Optional[str] = None
    customer_id: Optional[str] = None
    product_id: Optional[str] = None
    inventory_id: Optional[str] = None
    batch_id: Optional[str] = None
    quantity: Optional[float] = None
    unit_price: Optional[float] = None
    line_total: Optional[float] = None
    discount_amount: Optional[float] = None
    return_amount: Optional[float] = None
    description: Optional[str] = None
    is_sample: Optional[bool] = None
    is_promo: Optional[bool] = None
    process_date: Optional[datetime] = None


class SilverTicketSales(BaseModel):
    """ticket_sales"""

    store_id: str
    ticket_id: str
    org_id: Optional[str] = None
    org_name: Optional[str] = None
    store_name: Optional[str] = None
    store_url: Optional[str] = None
    external_store_id: Optional[str] = None
    analytics_id: Optional[str] = None
    date_close: Optional[datetime] = None
    date_open: Optional[datetime] = None
    ticket_type: Optional[str] = None
    revenue_source: Optional[str] = None
    ticket_source: Optional[str] = None
    customer_type: Optional[str] = None
    customer_id: Optional[str] = None
    is_sample: Optional[bool] = None
    is_promo: Optional[bool] = None
    units_sold: Optional[float] = None
    gross_sales: Optional[float] = None
    discounts: Optional[float] = None
    returns: Optional[float] = None
    net_sales: Optional[float] = None
    process_date: Optional[datetime] = None


class SilverTicketLineTaxes(BaseModel):
    """ticketline_taxes"""

    store_id: str
    tax_log_id: str
    ticket_id: Optional[str] = None
    ticketline_id: Optional[str] = None
    tax_amount: Optional[float] = None
    excise_tax_amount: Optional[float] = None
    sales_tax_amount: Optional[float] = None
    date_close: Optional[datetime] = None
    process_date: Optional[datetime] = None


class SilverTicketLineTaxTotals(BaseModel):
    """ticketline_tax_totals"""

    store_id: str
    ticket_id: str
    taxes: Optional[float] = None
    excise_taxes: Optional[float] = None
    sales_taxes: Optional[float] = None
    process_date: Optional[datetime] = None


class SilverCustomerFirstTxTimestamp(BaseModel):
    """customer_first_tx_timestamp"""

    store_id: str
    customer_id: str
    customer_first_ticket_timestamp: Optional[datetime] = None


class SilverCustomerFirstTx(BaseModel):
    """customer_first_tx"""

    store_id: str
    ticket_id: str
    customer_id: Optional[str] = None
    date_close: Optional[datetime] = None
    customer_first_ticket_timestamp: Optional[datetime] = None
    customer_visit_type: Optional[Literal["new", "returning", "walk_in"]] = None
    process_date: Optional[datetime] = None


class SilverDailyCustomerCnt(BaseModel):
    """daily_customer_cnt"""

    store_id: str
    sale_date: Optional[date] = None
    customer_count: Optional[int] = None
    process_date: Optional[datetime] = None


class SilverInvoiceLine(BaseModel):
    """invoice_line"""

    store_id: str
    invoice_line_cost_id: str
    ticket_id: Optional[str] = None
    ticketline_id: Optional[str] = None
    product_id: Optional[str] = None
    batch_id: Optional[str] = None
    cost_with_excise: Optional[float] = None
    cost_without_excise: Optional[float] = None
    true_cost_without_excise: Optional[float] = None
    base_cost: Optional[float] = None
    is_sample: Optional[bool] = None
    is_promo: Optional[bool] = None
    process_date: Optional[datetime] = None


class SilverInvoiceFact(BaseModel):
    """invoice_fact"""

    store_id: str
    ticket_id: str
    cost_with_excise: Optional[float] = None
    cost_without_excise: Optional[float] = None
    true_cost_without_excise: Optional[float] = None
    base_cost: Optional[float] = None
    sample_cost_with_excise: Optional[float] = None
    promo_cost_with_excise: Optional[float] = None
    process_date: Optional[datetime] = None


# Registry
SILVER_SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "ticketline_sales":              SilverTicketLineSales,
    "ticket_sales":                  SilverTicketSales,
    "ticketline_taxes":              SilverTicketLineTaxes,
    "ticketline_tax_totals":         SilverTicketLineTaxTotals,
    "customer_first_tx_timestamp":   SilverCustomerFirstTxTimestamp,
    "customer_first_tx":             SilverCustomerFirstTx,
    "daily_customer_cnt":            SilverDailyCustomerCnt,
    "invoice_line":                  SilverInvoiceLine,
    "invoice_fact":                  SilverInvoiceFact,
}
