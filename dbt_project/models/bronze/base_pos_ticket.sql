{{
  config(
    unique_key    = ['store_id', 'ticket_id'],
    cluster_by   = ['store_id', 'process_date']
  )
}}

/*
  Bronze: posper_ticket
  One row per POS ticket (transaction header), deduplicated to the latest CDC event.
  Deleted records are excluded so downstream silver models work on live data only.
*/

with

{% if is_incremental() %}
records_to_sync as (

    -- Phase 1: find every (store_id, record_key) that has a new CDC event
    -- since the last time this model ran.  COALESCE handles first-ever run.
    select distinct store_id, record_key
    from {{ source('debezium_staging', 'pos_staging') }}
    where table_name = 'posper_ticket'
      and load_date >= (
          select coalesce(max(load_date), '2000-01-01'::timestamp_ntz) from {{ this }}
      )

),
{% endif %}

raw as (

    -- Phase 2: pull ALL historical events for changed keys only, then dedup.
    select
        stg.store_id,
        stg.record_key                                     as ticket_id,
        stg.record_data,
        stg.debezium_op,
        stg.deleted,
        stg.sync_date,
        stg.process_date,
        stg.load_date
    from {{ source('debezium_staging', 'pos_staging') }} as stg
    where stg.table_name = 'posper_ticket'

    {% if is_incremental() %}
    and exists (
        select 1 from records_to_sync rts
        where rts.store_id = stg.store_id and rts.record_key = stg.record_key
    )
    {% endif %}

    -- Scope filters for range:/store: repair runs (is_incremental=False for those).
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
        partition by store_id, ticket_id
        order by sync_date desc
    ) = 1

),

parsed as (

    select
        store_id,
        ticket_id,

        -- Ticket header fields parsed from the VARIANT payload
        -- dateClose/dateOpen are camelCase epoch-milliseconds in the source JSON
        record_data:id::varchar                                                            as id,
        to_timestamp_ntz(get_ignore_case(record_data, 'dateclose')::number, 3)            as date_close,
        to_timestamp_ntz(get_ignore_case(record_data, 'dateopen')::number,  3)            as date_open,
        record_data:ticket_type::varchar               as ticket_type,
        record_data:revenue_source::varchar            as revenue_source,
        record_data:ticket_source::varchar             as ticket_source,
        record_data:customer_type::varchar             as customer_type,
        record_data:hints_customer_id::varchar         as customer_id,
        record_data:employee_id::varchar               as employee_id,

        -- Financial totals
        record_data:subtotal::number(18,4)             as subtotal,
        record_data:total_price::number(18,4)          as total_price,
        record_data:discount_amount::number(18,4)      as discount_amount,
        record_data:return_amount::number(18,4)        as return_amount,
        record_data:tier_discount_amount::number(18,4) as tier_discount_amount,
        record_data:tax_total::number(18,4)            as tax_total,
        record_data:gross_receipts::number(18,4)       as gross_receipts,

        -- Metadata
        sync_date,
        process_date,
        load_date

    from deduped
    where not coalesce(deleted, false)
      and debezium_op != 'd'

)

select * from parsed
