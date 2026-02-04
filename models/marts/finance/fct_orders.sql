SELECT 
    orders.order_id
    , orders.customer_id
    , SUM(payments.amount) AS amount
FROM 
    {{ ref('stg_stripe_payments') }} AS payments
    LEFT JOIN {{ ref('stg_jaffle_shop_orders') }} AS orders
    ON payments.order_id = orders.order_id
WHERE payments.status = 'success'
GROUP BY orders.order_id, orders.customer_id