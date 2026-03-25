with customers as (

    select *
    from {{ ref('stg_jaffle_shop__customers') }}    

),

orders as (

    select *
    from {{ ref('stg_jaffle_shop__orders') }} 

),

payments AS (
    SELECT *
    FROM {{ ref('stg_stripe__payments') }}
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

customer_payments AS (
    SELECT customer_id,
           COALESCE(SUM(payment_amount), 0) AS lifetime_value

    FROM orders AS o 
    LEFT JOIN payments AS p ON o.order_id = p.order_id

    GROUP BY 1
),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        COALESCE(lifetime_value, 0) AS lifetime_value

    from customers

    left join customer_orders using (customer_id)
    LEFT JOIN customer_payments USING (customer_id)

)

select * from final