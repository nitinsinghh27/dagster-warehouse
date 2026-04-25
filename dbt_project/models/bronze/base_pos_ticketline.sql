{{
  config(
    unique_key  = ['store_id', 'ticketline_id'],
    cluster_by  = ['store_id', 'process_date']
  )
}}

/*
  Bronze: posper_ticketline
  One row per ticket line item.
  The `description` field drives is_sample / is_promo classification in silver.
*/

with

{% if is_incremental() %}
records_to_sync as (

    select distinct store_id, record_key
    from {{ source('debezium_staging', 'pos_staging') }}
    where table_name = 'posper_ticketline'
      and load_date >= (
          select coalesce(max(load_date), '2000-01-01'::timestamp_ntz) from {{ this }}
      )

),
{% endif %}

raw as (

    select
        stg.store_id,
        stg.record_key          as ticketline_id,
        stg.record_data,
        stg.debezium_op,
        stg.deleted,
        stg.sync_date,
        stg.process_date,
        stg.load_date
    from {{ source('debezium_staging', 'pos_staging') }} as stg
    where stg.table_name = 'posper_ticketline'

    {% if is_incremental() %}
    and exists (
        select 1 from records_to_sync rts
        where rts.store_id = stg.store_id and rts.record_key = stg.record_key
    )
    {% endif %}

    {% if var('store_ids', []) | length > 0 %}
        and stg.store_id in (
            {%- for id in var('store_ids') -%}
                '{{ id }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        )
    {% endif %}

    {% if var('date_from', '') != '' %}
        and stg.process_date >= '{{ var('date_from') }}'::date
    {% endif %}

    {% if var('date_to', '') != '' %}
        and stg.process_date < dateadd('day', 1, '{{ var('date_to') }}'::date)
    {% endif %}

),

deduped as (

    select *
    from raw
    qualify row_number() over (
        partition by store_id, ticketline_id
        order by sync_date desc
    ) = 1

),

parsed as (

    select
        store_id,
        ticketline_id,
        record_data:id::varchar                    as id,
        record_data:ticket_ticketline::varchar     as ticket_id,
        record_data:product_id::varchar            as product_id,
        record_data:batch_id::varchar              as batch_id,
        record_data:description::varchar           as description,

        -- Inventory unit linkage (used to join invoice_line costs)
        record_data:unit_id::varchar               as unit_id,

        -- Quantities and pricing
        -- JSON uses camelCase: amount (qty), priceSell, priceSellOrig
        get_ignore_case(record_data, 'amount')::number(18,4)                                      as quantity,
        get_ignore_case(record_data, 'pricesell')::number(18,4)                                   as unit_price,
        (get_ignore_case(record_data, 'amount')::number(18,4)
            * get_ignore_case(record_data, 'pricesell')::number(18,4))                            as line_total,
        record_data:discount_amount::number(18,4)                                                  as discount_amount,
        record_data:return_amount::number(18,4)                                                    as return_amount,
        get_ignore_case(record_data, 'cost_per_unit')::string::number(18,4)                        as cost,

        -- Metadata
        sync_date,
        process_date,
        load_date

    from deduped
    where not coalesce(deleted, false)
      and debezium_op != 'd'

)

select * from parsed
