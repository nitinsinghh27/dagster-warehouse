{{
  config(
    unique_key  = ['store_id', 'ticket_id'],
    cluster_by  = ['store_id', 'date_close::date']
  )
}}

/*
  Silver: customer_first_tx
  Classifies each ticket visit as 'new' or 'returning' by comparing the ticket's
  date_close to the customer's very first transaction timestamp.

  customer_visit_type:
    'new'       — this ticket IS the customer's first transaction at this store
    'returning' — the customer has transacted before at this store
    'walk_in'   — ticket has no customer_id (anonymous / walk-in)
*/

with ticket as (

    select
        store_id,
        ticket_id,
        customer_id,
        date_close,
        process_date
    from {{ ref('base_pos_ticket') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

),

first_tx as (

    select * from {{ ref('customer_first_tx_timestamp') }}

)

select
    t.store_id,
    t.ticket_id,
    t.customer_id,
    t.date_close,
    ft.customer_first_ticket_timestamp,

    case
        when t.customer_id is null
            then 'walk_in'
        when t.date_close = ft.customer_first_ticket_timestamp
            then 'new'
        else 'returning'
    end as customer_visit_type,

    t.process_date

from ticket t
left join first_tx ft
    on  t.store_id    = ft.store_id
    and t.customer_id = ft.customer_id
