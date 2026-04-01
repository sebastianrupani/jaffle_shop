with

orders as (
    select *
    from {{ ref('int_orders') }}
),

customer_ltv as (
    select t1.order_id, sum(t2.total_amount_paid) as clv_bad
    
    from orders t1
    left join orders t2
           on t1.customer_id = t2.customer_id
          and t1.order_id >= t2.order_id
    
    group by 1
),

final as (
    select
        orders.*,
        row_number() over (order by orders.order_id) as transaction_seq,
        row_number() over (partition by customer_id 
                               order by orders.order_id) as customer_sales_seq,

        case when rank() over (partition by customer_id
                                   order by order_date, orders.order_id) = 1
             then 'new' 
             else 'return' end as nvsr,

        cltv.clv_bad as customer_lifetime_value,
        min(orders.order_date) over (partition by orders.customer_id) as first_order_date

    from orders
    left outer join customer_ltv as cltv
                on cltv.order_id = orders.order_id

    order by orders.order_id
)

select * from final