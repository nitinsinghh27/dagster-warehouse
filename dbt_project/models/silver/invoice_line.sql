{{
  config(
    unique_key  = ['store_id', 'invoice_line_cost_id'],
    cluster_by  = ['store_id', 'process_date']
  )
}}

/*
  Silver: invoice_line
  Joins invoice line cost data to ticketline_sales so that COGS columns
  (cost_with_excise, cost_without_excise, true_cost_without_excise, base_cost)
  are available at ticket-line granularity.
*/

with cost as (

    select * from {{ ref('base_invoice_line_cost') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

),

inventory as (

    select
        store_id,
        inventory_id,
        invoice_line_id,
        batch_id
    from {{ ref('base_inventory') }}

),

ticketline as (

    select
        store_id,
        ticketline_id,
        ticketline_uuid,
        ticket_id,
        unit_id,
        date_close,
        is_sample,
        is_promo
    from {{ ref('ticketline_sales') }}

)

select
    c.store_id,
    c.invoice_line_cost_id,
    c.invoice_line_id,
    c.batch_id,
    c.product_id,
    tl.ticketline_id,
    tl.ticket_id,
    tl.date_close,
    tl.is_sample,
    tl.is_promo,

    c.cost_with_excise,
    c.cost_without_excise,
    c.true_cost_without_excise,
    c.base_cost,
    c.quantity,

    c.process_date

from cost               c
-- join cost → inventory via invoice_line_id
inner join inventory    inv
    on  c.store_id        = inv.store_id
    and c.invoice_line_id = inv.invoice_line_id
-- join inventory → ticketline via unit_id (the specific inventory unit sold)
left join ticketline    tl
    on  inv.store_id     = tl.store_id
    and inv.inventory_id = tl.unit_id
