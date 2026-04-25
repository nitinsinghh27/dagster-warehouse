{{
  config(
    unique_key  = ['store_id', 'ticket_id'],
    cluster_by  = ['store_id', 'date_close::date']
  )
}}

/*
  Silver: ticketline_tax_totals
  Aggregates all tax amounts to ticket level.
  Separates excise taxes from regular taxes for margin calculations in gold.
*/

with base as (

    select * from {{ ref('ticketline_taxes') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

)

select
    store_id,
    ticket_id,
    date_close,

    sum(tax_amount)                                        as taxes,
    sum(case when is_excise then tax_amount else 0 end)    as excise_taxes,
    sum(case when not is_excise then tax_amount else 0 end) as sales_taxes,

    max(process_date)                                      as process_date

from base
group by
    store_id,
    ticket_id,
    date_close
