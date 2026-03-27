SELECT
    id AS payment_id,
    orderid AS order_id,
    INITCAP(REPLACE(paymentmethod, '_', ' ')) AS payment_method,
    INITCAP(status) AS payment_status,
    amount / 100 AS payment_amount,
    created AS payment_date

FROM {{ source('stripe', 'payments') }}