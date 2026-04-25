{{
  config(
    unique_key  = ['store_id', 'inventory_id'],
    cluster_by  = ['store_id', 'process_date']
  )
}}

/*
  Bronze: tz_inventory
  Inventory batch records with excise and non-excise cost data.
  Joined in silver to attach cost to ticket line items.
*/

with

{% if is_incremental() %}
records_to_sync as (

    select distinct store_id, record_key
    from {{ source('debezium_staging', 'pos_staging') }}
    where table_name = 'tz_inventory'
      and load_date >= (
          select coalesce(max(load_date), '2000-01-01'::timestamp_ntz) from {{ this }}
      )

),
{% endif %}

raw as (

    select
        stg.store_id,
        stg.record_key       as inventory_id,
        stg.record_data,
        stg.debezium_op,
        stg.deleted,
        stg.sync_date,
        stg.process_date,
        stg.load_date
    from {{ source('debezium_staging', 'pos_staging') }} as stg
    where stg.table_name = 'tz_inventory'

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
        partition by store_id, inventory_id
        order by sync_date desc
    ) = 1

),

parsed as (

    select
        store_id,
        inventory_id,
        record_data:product_id::varchar            as product_id,
        record_data:batch_id::varchar              as batch_id,
        record_data:invoice_line_id::varchar       as invoice_line_id,
        record_data:cost_with_excise::number(18,4) as cost_with_excise,
        record_data:cost_without_excise::number(18,4) as cost_without_excise,
        record_data:quantity_on_hand::number(18,4) as quantity_on_hand,

        sync_date,
        process_date,
        load_date

    from deduped
    where not coalesce(deleted, false)
      and debezium_op != 'd'

)

select * from parsed
