$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$sql = @'
DO $$
DECLARE
    base_ts TIMESTAMP := date_trunc('minute', CURRENT_TIMESTAMP) - INTERVAL '3 minutes';
    order_one_id BIGINT;
    order_two_id BIGINT;
    payment_one_id BIGINT;
    payment_two_id BIGINT;
BEGIN
    INSERT INTO orders (
        user_id,
        order_status,
        total_amount,
        currency,
        coupon_id,
        created_at,
        updated_at
    )
    VALUES (
        1004,
        'CREATED',
        89.90,
        'KRW',
        NULL,
        base_ts + INTERVAL '10 seconds',
        base_ts + INTERVAL '10 seconds'
    )
    RETURNING order_id INTO order_one_id;

    INSERT INTO orders (
        user_id,
        order_status,
        total_amount,
        currency,
        coupon_id,
        created_at,
        updated_at
    )
    VALUES (
        1005,
        'CREATED',
        245.00,
        'KRW',
        'FLASH15',
        base_ts + INTERVAL '70 seconds',
        base_ts + INTERVAL '70 seconds'
    )
    RETURNING order_id INTO order_two_id;

    INSERT INTO payments (
        order_id,
        user_id,
        payment_status,
        payment_method,
        amount,
        paid_at,
        updated_at
    )
    VALUES (
        order_one_id,
        1004,
        'SUCCEEDED',
        'CARD',
        89.90,
        base_ts + INTERVAL '20 seconds',
        base_ts + INTERVAL '20 seconds'
    )
    RETURNING payment_id INTO payment_one_id;

    INSERT INTO payments (
        order_id,
        user_id,
        payment_status,
        payment_method,
        amount,
        paid_at,
        updated_at
    )
    VALUES (
        order_two_id,
        1005,
        'PENDING',
        'CARD',
        245.00,
        base_ts + INTERVAL '80 seconds',
        base_ts + INTERVAL '80 seconds'
    )
    RETURNING payment_id INTO payment_two_id;

    UPDATE payments
    SET
        payment_status = 'SUCCEEDED',
        paid_at = base_ts + INTERVAL '90 seconds'
    WHERE payment_id = payment_two_id;

    INSERT INTO refunds (
        order_id,
        payment_id,
        user_id,
        refund_status,
        refund_reason,
        refund_amount,
        refunded_at,
        updated_at
    )
    VALUES (
        order_one_id,
        payment_one_id,
        1004,
        'SUCCEEDED',
        'CUSTOMER_CHANGED_MIND',
        89.90,
        base_ts + INTERVAL '110 seconds',
        base_ts + INTERVAL '110 seconds'
    );
END $$;

SELECT 'orders' AS entity, order_id AS id, order_status AS status, created_at AS event_ts
FROM orders
ORDER BY order_id DESC
LIMIT 2;

SELECT 'payments' AS entity, payment_id AS id, payment_status AS status, paid_at AS event_ts
FROM payments
ORDER BY payment_id DESC
LIMIT 2;

SELECT 'refunds' AS entity, refund_id AS id, refund_status AS status, refunded_at AS event_ts
FROM refunds
ORDER BY refund_id DESC
LIMIT 1;
'@

$sql | docker compose exec -T postgres psql -U postgres -d commerce -v ON_ERROR_STOP=1
