select
  customer_id,
  COUNT(customer_id) AS occurences

from {{ ref('dim_customers') }}
group by 1
having COUNT(customer_id) > 1