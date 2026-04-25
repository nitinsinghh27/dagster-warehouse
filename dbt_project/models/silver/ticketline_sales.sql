{{
  config(
    unique_key  = ['store_id', 'ticketline_id'],
    cluster_by  = ['store_id', 'date_close::date']
  )
}}

/*
  Silver: ticketline_sales
  Enriched ticket line items.  Joins ticketline + ticket header + tenant to attach
  all dimensions needed for gold aggregations.
  Derives is_sample / is_promo from the line description field.
*/

with ticketline as (

    select * from {{ ref('base_pos_ticketline') }}

    {% if is_incremental() %}
        where process_date >= (
            select dateadd('day', -1, max(process_date)) from {{ this }}
        )
    {% endif %}

),

ticket as (

    select
        store_id,
        ticket_id,
        id,
        date_close,
        date_open,
        ticket_type,
        revenue_source,
        ticket_source,
        customer_type,
        customer_id
    from {{ ref('base_pos_ticket') }}

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

joined as (

    select
        tl.store_id,
        tl.ticketline_id,
        tl.id             as ticketline_uuid,
        tl.ticket_id,
        ten.org_id,
        ten.org_name,
        ten.store_name,
        ten.store_url,
        ten.external_store_id,
        ten.analytics_id,

        -- Ticket dimensions
        t.date_close,
        t.date_open,
        t.ticket_type,
        t.revenue_source,
        t.ticket_source,
        t.customer_type,
        t.customer_id,

        -- Line item fields
        tl.product_id,
        tl.unit_id,
        tl.batch_id,
        tl.description,
        tl.quantity,
        tl.unit_price,
        tl.line_total,
        tl.discount_amount,
        tl.return_amount,
        tl.cost,

        -- Derived flags: description-based classification
        case
            when lower(tl.description) like '%sample%'  then true
            else false
        end                                              as is_sample,

        case
            when lower(tl.description) like '%promo%'
              or lower(tl.description) like '%promotional%' then true
            else false
        end                                              as is_promo,

        tl.process_date

    from ticketline     tl
    inner join ticket   t   on  tl.store_id  = t.store_id
                            and tl.ticket_id = t.id
    inner join tenant   ten on  tl.store_id  = ten.store_id

)

select * from joined
