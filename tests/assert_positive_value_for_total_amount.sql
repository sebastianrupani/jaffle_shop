SELECT payment_status, 
       SUM(payment_amount)

FROM {{ ref('stg_stripe__payments') }}

WHERE payment_status = 'Success'

GROUP BY 1
HAVING SUM(payment_amount) < 0