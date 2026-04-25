{{
  config(
    unique_key  = ['store_id', 'tax_log_id'],
    cluster_by  = ['store_id', 'process_date']
  )
}}

/*
  Silver: ticketline_taxes
  Enriched tax log — joins the raw tax events to ticketline_sales to inherit
  all ticket-level dimensions.  Used by ticketline_tax_totals.
*/

with tax_log as (

    select * from {{ ref('base_pos_ticketline_tax_log') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

),

ticketline as (

    select
        store_id,
        ticketline_id,
        ticketline_uuid,
        ticket_id,
        date_close
    from {{ ref('ticketline_sales') }}

)

select
    t.store_id,
    t.tax_log_id,
    t.ticket_id,
    t.ticketline_id,
    tl.date_close,
    t.tax_name,
    t.tax_rate,
    t.tax_amount,
    t.is_excise,
    t.process_date

from tax_log      t
inner join ticketline tl
    on  t.store_id      = tl.store_id
    and t.ticketline_id = tl.ticketline_uuid
