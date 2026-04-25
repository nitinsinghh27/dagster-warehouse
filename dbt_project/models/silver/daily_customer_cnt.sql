{{
  config(
    unique_key  = ['store_id', 'sale_date'],
    cluster_by  = ['store_id', 'sale_date']
  )
}}

/*
  Silver: daily_customer_cnt
  Unique customer count per store per day.
  Used to validate and cross-check customer counts in gold.
*/

with ticket as (

    select
        store_id,
        customer_id,
        date_close::date as sale_date,
        process_date
    from {{ ref('base_pos_ticket') }}
    where customer_id is not null
      and date_close  is not null

    {% if is_incremental() %}
        and process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

)

select
    store_id,
    sale_date,
    count(distinct customer_id) as unique_customer_count,
    max(process_date)           as process_date
from ticket
group by store_id, sale_date
