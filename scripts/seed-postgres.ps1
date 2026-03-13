$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$sql = @"
INSERT INTO orders (user_id, order_status, total_amount, currency, coupon_id)
VALUES
    (1004, 'CREATED', 89.90, 'KRW', NULL),
    (1005, 'CREATED', 245.00, 'KRW', 'FLASH15');

INSERT INTO payments (order_id, user_id, payment_status, payment_method, amount)
VALUES
    (4, 1004, 'SUCCEEDED', 'CARD', 89.90),
    (5, 1005, 'SUCCEEDED', 'CARD', 245.00);

UPDATE orders
SET order_status = 'PAID'
WHERE order_id IN (4, 5);

INSERT INTO refunds (order_id, payment_id, user_id, refund_status, refund_reason, refund_amount)
VALUES
    (4, 3, 1004, 'SUCCEEDED', 'CUSTOMER_CHANGED_MIND', 89.90);
"@

$sql | docker compose exec -T postgres psql -U postgres -d commerce -v ON_ERROR_STOP=1
