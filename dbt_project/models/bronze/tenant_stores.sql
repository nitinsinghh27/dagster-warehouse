{{
  config(
    unique_key  = ['store_id'],
    cluster_by  = ['store_id']
  )
}}

/*
  Bronze: tenant_stores
  Store / tenant dimension built from the platform organisation service CDC tables.
  One row per active store with org linkage, analytics identifiers, and display metadata.

  Sources (both in INGEST.DEBEZIUM_STAGING):
    PLATFORM_ORGANIZATION_ENTITY       — store-level records (store_id, org_id, analytics_id, …)
    PLATFORM_ORGANIZATION_ORGANIZATION — org-level records  (org_id, org_name)

  Note: store_url and external_store_id are stored as entries in the INTERNAL_IDS
  JSON array and are extracted via LATERAL FLATTEN + conditional aggregation.
*/

with

entities as (

    select
        e.id::varchar                                              as store_id,
        e.organization_id::varchar                                 as org_id,
        e.name::varchar                                            as store_name,
        e.analyticsid::varchar                                     as analytics_id,
        e.internal_ids                                             as internal_ids_raw,
        to_timestamp_ntz(e.__source_ts_ms / 1000)                 as sync_date,
        e.__deleted                                                as deleted
    from {{ source('debezium_staging', 'platform_organization_entity') }} as e

    {% if is_incremental() %}
    where to_timestamp_ntz(e.__source_ts_ms / 1000) >= (
        select coalesce(max(sync_date), '2000-01-01'::timestamp_ntz) from {{ this }}
    )
    {% endif %}

    qualify row_number() over (
        partition by e.id
        order by e.__source_ts_ms desc
    ) = 1

),

orgs as (

    -- Always full-scan: small table, needed to resolve org_name for any changed entity.
    select
        o.id::varchar                                              as org_id,
        o.name::varchar                                            as org_name
    from {{ source('debezium_staging', 'platform_organization_organization') }} as o
    qualify row_number() over (
        partition by o.id
        order by o.__source_ts_ms desc
    ) = 1

),

flattened as (

    -- Extract store_url and external_store_id from the INTERNAL_IDS JSON array.
    -- OUTER => TRUE preserves entities with null / empty internal_ids.
    select
        e.store_id,
        e.org_id,
        o.org_name,
        e.store_name,
        e.analytics_id,
        e.sync_date,
        max(case when f.value:source::string = 'storeUrl'
                 then f.value:sourceId::string end)                as store_url,
        max(case when f.value:source::string = 'datalakeStoreId'
                 then f.value:sourceId::string end)                as external_store_id
    from entities as e
    left join orgs as o
        on e.org_id = o.org_id,
    lateral flatten(input => parse_json(e.internal_ids_raw), outer => true) as f
    where not coalesce(e.deleted, false)
    group by
        e.store_id, e.org_id, o.org_name, e.store_name, e.analytics_id, e.sync_date

)

select * from flattened
