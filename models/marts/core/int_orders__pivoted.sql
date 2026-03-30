WITH payments AS (
    SELECT *
    FROM {{ ref('stg_stripe__payments') }}
    WHERE payment_status = 'Success'
),

pivoted AS (
    SELECT order_id,
    {% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}
    {%- for payment_method in payment_methods -%}

        SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN payment_amount ELSE 0 END) AS {{ payment_method}}_amount 

        {%- if not loop.last -%}
            , 
        {% endif %}    
    {% endfor %}

    FROM payments
    GROUP BY 1
)

SELECT *
FROM pivoted
ORDER BY 1 