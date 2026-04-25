{{
  config(
    unique_key  = ['store_id', 'ticket_id'],
    cluster_by  = ['store_id', 'date_close::date']
  )
}}

/*
  Silver: invoice_fact
  Aggregates all invoice line costs to ticket level.
  The gold model joins this to attach the four cost columns to every ticket.
*/

with base as (

    select * from {{ ref('invoice_line') }}

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

    sum(cost_with_excise)                                           as cost_with_excise,
    sum(cost_without_excise)                                        as cost_without_excise,
    sum(true_cost_without_excise)                                   as true_cost_without_excise,
    sum(base_cost)                                                  as base_cost,

    -- Sample / promo cost splits — used for margin reporting
    sum(case when is_sample then cost_with_excise else 0 end)       as sample_cost_with_excise,
    sum(case when is_promo  then cost_with_excise else 0 end)       as promo_cost_with_excise,

    max(process_date)                                               as process_date

from base
group by
    store_id,
    ticket_id,
    date_close
