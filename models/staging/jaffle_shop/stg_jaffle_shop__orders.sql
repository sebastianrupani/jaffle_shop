select distinct
    id as order_id,
    user_id as customer_id,
    order_date,
    status as order_status,
    case when order_status not in ('returned', 'pending') 
         then order_date end as valid_order_date

from {{ source('jaffle_shop', 'orders')}}