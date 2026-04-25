{{
  config(
    unique_key  = ['store_id', 'tax_log_id'],
    cluster_by  = ['store_id', 'process_date']
  )
}}

/*
  Bronze: crm_area_tax_ticketline_log
  Per-line tax event log captured by Debezium.
  Feeds silver ticketline_taxes and ticketline_tax_totals.
*/

with

{% if is_incremental() %}
records_to_sync as (

    select distinct store_id, record_key
    from {{ source('debezium_staging', 'pos_staging') }}
    where table_name = 'crm_area_tax_ticketline_log'
      and load_date >= (
          select coalesce(max(load_date), '2000-01-01'::timestamp_ntz) from {{ this }}
      )

),
{% endif %}

raw as (

    select
        stg.store_id,
        stg.record_key       as tax_log_id,
        stg.record_data,
        stg.debezium_op,
        stg.deleted,
        stg.sync_date,
        stg.process_date,
        stg.load_date
    from {{ source('debezium_staging', 'pos_staging') }} as stg
    where stg.table_name = 'crm_area_tax_ticketline_log'

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
        partition by store_id, tax_log_id
        order by sync_date desc
    ) = 1

),

parsed as (

    select
        store_id,
        tax_log_id,
        record_data:ticket_id::varchar             as ticket_id,
        record_data:ticketline_id::varchar         as ticketline_id,
        record_data:tax_name::varchar              as tax_name,
        record_data:tax_rate::number(10,6)         as tax_rate,
        record_data:tax_amount::number(18,4)       as tax_amount,
        record_data:is_excise::boolean             as is_excise,

        sync_date,
        process_date,
        load_date

    from deduped
    where not coalesce(deleted, false)
      and debezium_op != 'd'

)

select * from parsed
