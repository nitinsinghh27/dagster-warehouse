{{
  config(
    unique_key  = ['store_id', 'customer_id'],
    cluster_by  = ['store_id']
  )
}}

/*
  Silver: customer_first_tx_timestamp
  Records the very first completed-ticket timestamp per customer per store.
  Used by customer_first_tx to classify visits as new vs. returning.
*/

with ticket as (

    select
        store_id,
        customer_id,
        date_close
    from {{ ref('base_pos_ticket') }}
    where customer_id is not null
      and date_close  is not null

)

select
    store_id,
    customer_id,
    min(date_close) as customer_first_ticket_timestamp
from ticket
group by store_id, customer_id
