{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with customers as (
    select *
    from {{ ref('stg_jaffle_shop__customers') }}    
),

orders as (
    select *
    from {{ ref('stg_jaffle_shop__orders') }} 
),

payments as (
    select *
    from {{ ref('stg_stripe__payments') }} 
),

final as (
    select o.order_id,
        c.customer_id,
        o.order_date as location_ordered_at, 
        coalesce(sum(p.payment_amount), 0) as order_amount 

    from customers as c
    join orders as o on c.customer_id = o.customer_id
    join payments as p on o.order_id = p.order_id

    group by 1, 2, 3
    order by 2, 1, 3
)

select * from final
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where location_ordered_at > (select max(location_ordered_at) from {{ this }}) 
{% endif %}