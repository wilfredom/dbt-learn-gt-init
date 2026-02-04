SELECT
    id AS operation_id
    , orderid AS order_id
    , created AS order_date
    , paymentmethod AS payment_method
    , status
    , amount
FROM raw.stripe.payment
