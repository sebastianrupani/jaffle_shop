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
)

SELECT o.order_id,
       c.customer_id,
       COALESCE(SUM(p.payment_amount), 0) AS amount 

FROM customers AS c
JOIN orders AS o ON c.customer_id = o.customer_id
JOIN payments AS p ON o.order_id = p.order_id

GROUP BY 1, 2
ORDER BY 2, 1