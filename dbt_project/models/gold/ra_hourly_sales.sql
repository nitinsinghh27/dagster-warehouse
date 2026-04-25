{{
  config(
    unique_key     = 'synth_key',
    cluster_by     = ['store_id', 'hour_close::date']
  )
}}

/*
  Gold: ra_hourly_sales
  Retail Analytics hourly sales report.

  Grain: one row per (store × hour × ticket_type × revenue_source × customer_type
                       × customer_visit_type × is_sample × is_promo)

  synth_key is an MD5 over all grain columns so upserts are idempotent.

  Incremental window: re-processes the last 2 hours on every run to absorb
  late-arriving CDC events without a full rebuild.
*/

with ticket as (

    select * from {{ ref('ticket_sales') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(last_sync)) from {{ this }}
        )
    {% endif %}

),

customer_visit as (

    select
        store_id,
        ticket_id,
        customer_visit_type
    from {{ ref('customer_first_tx') }}

),

tax as (

    select
        store_id,
        ticket_id,
        taxes,
        excise_taxes,
        sales_taxes
    from {{ ref('ticketline_tax_totals') }}

),

cost as (

    select
        store_id,
        ticket_id,
        cost_with_excise,
        cost_without_excise,
        true_cost_without_excise,
        base_cost,
        sample_cost_with_excise,
        promo_cost_with_excise
    from {{ ref('invoice_fact') }}

),

tenant as (

    select
        store_id,
        org_id,
        org_name,
        store_name,
        store_url,
        external_store_id,
        analytics_id
    from {{ ref('tenant_stores') }}

),

enriched as (

    select
        t.store_id,
        ten.org_id,
        ten.org_name,
        ten.store_name,
        ten.store_url,
        ten.external_store_id,
        ten.analytics_id,

        -- Hour dimension: truncate date_close to the hour
        date_trunc('hour', t.date_close)                as hour_close,

        t.ticket_type,
        t.ticket_source,
        t.revenue_source,
        t.customer_type,
        coalesce(cv.customer_visit_type, 'walk_in')     as customer_visit_type,
        t.is_sample,
        t.is_promo,

        -- Ticket metrics
        t.customer_id,
        t.ticket_id,
        t.units_sold,
        t.gross_sales,
        t.discounts,
        t.returns,
        coalesce(t.discounts, 0) - coalesce(t.returns, 0) -- tier_discounts approximation
                                                        as tier_discounts,
        t.net_sales,

        -- Tax metrics
        coalesce(tx.taxes, 0)                           as taxes,
        coalesce(tx.excise_taxes, 0)                    as excise_taxes,

        -- Gross receipts = net_sales + taxes
        t.net_sales + coalesce(tx.taxes, 0)             as gross_receipts,

        -- Cost metrics
        coalesce(c.cost_with_excise, 0)                 as cost_with_excise,
        coalesce(c.cost_without_excise, 0)              as cost_without_excise,
        coalesce(c.true_cost_without_excise, 0)         as true_cost_without_excise,
        coalesce(c.base_cost, 0)                        as base_cost,
        coalesce(c.sample_cost_with_excise, 0)          as sample_cost_with_excise,
        coalesce(c.promo_cost_with_excise, 0)           as promo_cost_with_excise,

        t.process_date

    from ticket             t
    inner join tenant       ten on  t.store_id  = ten.store_id
    left  join customer_visit cv on  t.store_id  = cv.store_id
                                 and t.ticket_id  = cv.ticket_id
    left  join tax          tx  on  t.store_id  = tx.store_id
                                 and t.ticket_id  = tx.ticket_id
    left  join cost         c   on  t.store_id  = c.store_id
                                 and t.ticket_id  = c.ticket_id

),

aggregated as (

    select
        -- Grain key
        md5(concat_ws('|',
            org_id,
            store_id,
            revenue_source,
            hour_close::varchar,
            ticket_type,
            is_sample::varchar,
            customer_visit_type,
            customer_type,
            is_promo::varchar
        ))                                              as synth_key,

        -- Identifiers / dimensions
        org_id,
        store_id,
        analytics_id,
        org_name,
        store_name,
        store_url,
        external_store_id,
        hour_close,
        ticket_type,
        ticket_source,
        revenue_source,
        customer_type,
        customer_visit_type,
        is_sample,
        is_promo,

        -- Counts
        count(distinct customer_id)                     as customer_count,
        count(distinct ticket_id)                       as ticket_count,

        -- Volume
        sum(units_sold)                                 as units_sold,

        -- Revenue
        sum(gross_sales)                                as gross_sales,
        sum(net_sales)                                  as net_sales,
        sum(discounts)                                  as discounts,
        sum(returns)                                    as returns,
        sum(tier_discounts)                             as tier_discounts,
        sum(gross_receipts)                             as gross_receipts,

        -- Taxes
        sum(taxes)                                      as taxes,

        -- COGS
        sum(cost_with_excise)                           as cost_with_excise,
        sum(cost_without_excise)                        as cost_without_excise,
        sum(true_cost_without_excise)                   as true_cost_without_excise,
        sum(base_cost)                                  as base_cost,
        sum(sample_cost_with_excise)                    as sample_cost_with_excise,
        sum(promo_cost_with_excise)                     as promo_cost_with_excise,

        -- Freshness
        max(process_date)                               as last_sync

    from enriched
    group by
        org_id,
        store_id,
        analytics_id,
        org_name,
        store_name,
        store_url,
        external_store_id,
        hour_close,
        ticket_type,
        ticket_source,
        revenue_source,
        customer_type,
        customer_visit_type,
        is_sample,
        is_promo

)

select * from aggregated
