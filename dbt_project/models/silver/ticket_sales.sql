{{
  config(
    unique_key  = ['store_id', 'ticket_id'],
    cluster_by  = ['store_id', 'date_close::date']
  )
}}

/*
  Silver: ticket_sales
  Ticket-level aggregation from ticketline_sales.
  Aggregates quantities, revenue, and discount totals per ticket.
  Preserves all ticket-level dimensions passed through from bronze.
*/

with base as (

    select * from {{ ref('ticketline_sales') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

)

select
    store_id,
    ticket_id,
    org_id,
    org_name,
    store_name,
    store_url,
    external_store_id,
    analytics_id,
    date_close,
    date_open,
    ticket_type,
    revenue_source,
    ticket_source,
    customer_type,
    customer_id,

    -- is_sample / is_promo: true when ANY line on the ticket is flagged
    boolor_agg(is_sample)                                as is_sample,
    boolor_agg(is_promo)                                 as is_promo,

    -- Aggregated line metrics
    sum(quantity)                                        as units_sold,
    sum(line_total)                                      as gross_sales,
    sum(discount_amount)                                 as discounts,
    sum(return_amount)                                   as returns,
    sum(line_total) - sum(discount_amount)
        - sum(return_amount)                             as net_sales,

    max(process_date)                                    as process_date

from base
group by
    store_id,
    ticket_id,
    org_id,
    org_name,
    store_name,
    store_url,
    external_store_id,
    analytics_id,
    date_close,
    date_open,
    ticket_type,
    revenue_source,
    ticket_source,
    customer_type,
    customer_id
