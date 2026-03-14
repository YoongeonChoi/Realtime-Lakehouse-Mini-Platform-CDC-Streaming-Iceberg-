SET 'pipeline.name' = 'commerce_medallion_pipeline';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'table.local-time-zone' = 'Asia/Seoul';

CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.region' = 'us-east-1',
  's3.access-key-id' = 'minio',
  's3.secret-access-key' = 'minio12345'
);

USE CATALOG lakehouse;

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.orders_cdc (
  order_id BIGINT,
  user_id BIGINT,
  order_status STRING,
  total_amount DECIMAL(12, 2),
  currency STRING,
  coupon_id STRING,
  created_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  cdc_op STRING,
  cdc_source_ts TIMESTAMP(3),
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2',
  'write.distribution-mode' = 'hash'
);

CREATE TABLE IF NOT EXISTS bronze.payments_cdc (
  payment_id BIGINT,
  order_id BIGINT,
  user_id BIGINT,
  payment_status STRING,
  payment_method STRING,
  amount DECIMAL(12, 2),
  paid_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  cdc_op STRING,
  cdc_source_ts TIMESTAMP(3),
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2',
  'write.distribution-mode' = 'hash'
);

CREATE TABLE IF NOT EXISTS bronze.refunds_cdc (
  refund_id BIGINT,
  order_id BIGINT,
  payment_id BIGINT,
  user_id BIGINT,
  refund_status STRING,
  refund_reason STRING,
  refund_amount DECIMAL(12, 2),
  refunded_at TIMESTAMP(3),
  updated_at TIMESTAMP(3),
  cdc_op STRING,
  cdc_source_ts TIMESTAMP(3),
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2',
  'write.distribution-mode' = 'hash'
);

CREATE TABLE IF NOT EXISTS bronze.user_behavior_events (
  event_id STRING,
  user_id BIGINT,
  session_id STRING,
  event_type STRING,
  page STRING,
  product_id BIGINT,
  keyword STRING,
  cart_id STRING,
  order_id BIGINT,
  quantity INT,
  price DOUBLE,
  device_type STRING,
  source STRING,
  event_time TIMESTAMP(3),
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2',
  'write.distribution-mode' = 'hash'
);

CREATE TABLE IF NOT EXISTS bronze.crypto_ticks (
  tick_id STRING,
  symbol STRING,
  exchange STRING,
  price DOUBLE,
  volume DOUBLE,
  side STRING,
  trade_id STRING,
  trade_notional DOUBLE,
  ingest_source STRING,
  event_time TIMESTAMP(3),
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2',
  'write.distribution-mode' = 'hash'
);

CREATE TABLE IF NOT EXISTS silver.order_events (
  event_id STRING,
  user_id BIGINT,
  entity_type STRING,
  event_status STRING,
  amount DECIMAL(12, 2),
  event_time TIMESTAMP(3),
  source_system STRING,
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS silver.crypto_ticks_1s (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  symbol STRING,
  exchange STRING,
  tick_count BIGINT,
  min_price DOUBLE,
  max_price DOUBLE,
  avg_price DOUBLE,
  traded_volume DOUBLE,
  traded_notional DOUBLE,
  vwap DOUBLE,
  buy_tick_count BIGINT,
  sell_tick_count BIGINT,
  produced_at TIMESTAMP(3)
)
WITH (
  'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS silver.user_behavior (
  event_id STRING,
  user_id BIGINT,
  session_id STRING,
  event_type STRING,
  page STRING,
  product_id BIGINT,
  keyword STRING,
  order_id BIGINT,
  quantity INT,
  price DOUBLE,
  device_type STRING,
  source STRING,
  event_time TIMESTAMP(3),
  ingest_time TIMESTAMP(3)
)
WITH (
  'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS gold.crypto_market_kpis_1m (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  symbol STRING,
  metric_name STRING,
  metric_value DOUBLE,
  produced_at TIMESTAMP(3)
)
WITH (
  'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS gold.commerce_kpis_1m (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  metric_name STRING,
  metric_value DOUBLE,
  produced_at TIMESTAMP(3)
)
WITH (
  'format-version' = '2'
);

CREATE TABLE IF NOT EXISTS gold.behavior_kpis_1m (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  metric_name STRING,
  metric_value DOUBLE,
  produced_at TIMESTAMP(3)
)
WITH (
  'format-version' = '2'
);

CREATE TEMPORARY TABLE orders_cdc_source (
  order_id BIGINT,
  user_id BIGINT,
  order_status STRING,
  total_amount DECIMAL(12, 2),
  currency STRING,
  coupon_id STRING,
  created_at_ms BIGINT,
  updated_at_ms BIGINT,
  __op STRING,
  __source_ts_ms BIGINT,
  created_at AS TO_TIMESTAMP_LTZ(created_at_ms, 3),
  updated_at AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
  event_time AS TO_TIMESTAMP_LTZ(COALESCE(__source_ts_ms, updated_at_ms, created_at_ms), 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.cdc.commerce.public.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-orders-cdc',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE payments_cdc_source (
  payment_id BIGINT,
  order_id BIGINT,
  user_id BIGINT,
  payment_status STRING,
  payment_method STRING,
  amount DECIMAL(12, 2),
  paid_at_ms BIGINT,
  updated_at_ms BIGINT,
  __op STRING,
  __source_ts_ms BIGINT,
  paid_at AS TO_TIMESTAMP_LTZ(paid_at_ms, 3),
  updated_at AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
  event_time AS TO_TIMESTAMP_LTZ(COALESCE(__source_ts_ms, updated_at_ms, paid_at_ms), 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.cdc.commerce.public.payments',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-payments-cdc',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE refunds_cdc_source (
  refund_id BIGINT,
  order_id BIGINT,
  payment_id BIGINT,
  user_id BIGINT,
  refund_status STRING,
  refund_reason STRING,
  refund_amount DECIMAL(12, 2),
  refunded_at_ms BIGINT,
  updated_at_ms BIGINT,
  __op STRING,
  __source_ts_ms BIGINT,
  refunded_at AS TO_TIMESTAMP_LTZ(refunded_at_ms, 3),
  updated_at AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
  event_time AS TO_TIMESTAMP_LTZ(COALESCE(__source_ts_ms, updated_at_ms, refunded_at_ms), 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.cdc.commerce.public.refunds',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-refunds-cdc',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE behavior_events_source (
  event_id STRING NOT NULL,
  user_id BIGINT NOT NULL,
  session_id STRING NOT NULL,
  event_type STRING NOT NULL,
  page STRING NOT NULL,
  product_id BIGINT,
  keyword STRING,
  cart_id STRING,
  order_id BIGINT,
  quantity INT,
  price DOUBLE,
  device_type STRING NOT NULL,
  source STRING NOT NULL,
  kafka_record_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  event_time AS kafka_record_ts,
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.event.commerce.user_behavior_v3',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-user-behavior',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE crypto_ticks_source (
  tick_id STRING NOT NULL,
  symbol STRING NOT NULL,
  exchange STRING NOT NULL,
  price DOUBLE NOT NULL,
  volume DOUBLE NOT NULL,
  side STRING NOT NULL,
  trade_id STRING NOT NULL,
  ingest_source STRING NOT NULL,
  event_time BIGINT NOT NULL,
  event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  trade_notional AS price * volume,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.event.market.crypto_ticks_v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-crypto-ticks',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO bronze.orders_cdc
SELECT order_id, user_id, order_status, total_amount, currency, coupon_id, CAST(created_at AS TIMESTAMP(3)), CAST(updated_at AS TIMESTAMP(3)), __op, CAST(TO_TIMESTAMP_LTZ(__source_ts_ms, 3) AS TIMESTAMP(3)), CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM orders_cdc_source;

INSERT INTO bronze.payments_cdc
SELECT payment_id, order_id, user_id, payment_status, payment_method, amount, CAST(paid_at AS TIMESTAMP(3)), CAST(updated_at AS TIMESTAMP(3)), __op, CAST(TO_TIMESTAMP_LTZ(__source_ts_ms, 3) AS TIMESTAMP(3)), CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM payments_cdc_source;

INSERT INTO bronze.refunds_cdc
SELECT refund_id, order_id, payment_id, user_id, refund_status, refund_reason, refund_amount, CAST(refunded_at AS TIMESTAMP(3)), CAST(updated_at AS TIMESTAMP(3)), __op, CAST(TO_TIMESTAMP_LTZ(__source_ts_ms, 3) AS TIMESTAMP(3)), CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM refunds_cdc_source;

INSERT INTO bronze.crypto_ticks
SELECT
  tick_id,
  symbol,
  exchange,
  price,
  volume,
  side,
  trade_id,
  trade_notional,
  ingest_source,
  CAST(event_ts AS TIMESTAMP(3)),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM crypto_ticks_source;

INSERT INTO silver.order_events
SELECT event_id, user_id, entity_type, event_status, amount, event_time, source_system, ingest_time
FROM (
  SELECT
    CAST(order_id AS STRING) AS event_id,
    user_id,
    'order' AS entity_type,
    order_status AS event_status,
    total_amount AS amount,
    event_time,
    'postgres_cdc' AS source_system,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS ingest_time
  FROM orders_cdc_source

  UNION ALL

  SELECT
    CAST(payment_id AS STRING) AS event_id,
    user_id,
    'payment' AS entity_type,
    payment_status AS event_status,
    amount,
    event_time,
    'postgres_cdc' AS source_system,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS ingest_time
  FROM payments_cdc_source

  UNION ALL

  SELECT
    CAST(refund_id AS STRING) AS event_id,
    user_id,
    'refund' AS entity_type,
    refund_status AS event_status,
    refund_amount AS amount,
    event_time,
    'postgres_cdc' AS source_system,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS ingest_time
  FROM refunds_cdc_source
);

INSERT INTO silver.crypto_ticks_1s
SELECT
  window_start,
  window_end,
  symbol,
  exchange,
  COUNT(*) AS tick_count,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  AVG(price) AS avg_price,
  SUM(volume) AS traded_volume,
  SUM(trade_notional) AS traded_notional,
  CASE
    WHEN SUM(volume) = 0 THEN 0.0
    ELSE SUM(trade_notional) / SUM(volume)
  END AS vwap,
  SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buy_tick_count,
  SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_tick_count,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS produced_at
FROM TABLE(TUMBLE(TABLE crypto_ticks_source, DESCRIPTOR(event_ts), INTERVAL '1' SECOND))
GROUP BY window_start, window_end, symbol, exchange;

INSERT INTO gold.commerce_kpis_1m
SELECT window_start, window_end, metric_name, metric_value, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM (
  SELECT window_start, window_end, 'orders_created' AS metric_name, CAST(COUNT(*) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE orders_cdc_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
  WHERE __op = 'c'
  GROUP BY window_start, window_end

  UNION ALL

  SELECT window_start, window_end, 'gross_order_value' AS metric_name, CAST(COALESCE(SUM(total_amount), 0) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE orders_cdc_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
  WHERE __op = 'c'
  GROUP BY window_start, window_end

  UNION ALL

  SELECT window_start, window_end, 'payments_succeeded' AS metric_name, CAST(COUNT(*) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE payments_cdc_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
  WHERE payment_status = 'SUCCEEDED'
  GROUP BY window_start, window_end

  UNION ALL

  SELECT window_start, window_end, 'refund_amount' AS metric_name, CAST(COALESCE(SUM(refund_amount), 0) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE refunds_cdc_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end
);

INSERT INTO gold.crypto_market_kpis_1m
SELECT window_start, window_end, symbol, metric_name, metric_value, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM (
  SELECT
    window_start,
    window_end,
    symbol,
    'tick_count_1m' AS metric_name,
    CAST(COUNT(*) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE crypto_ticks_source, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, symbol

  UNION ALL

  SELECT
    window_start,
    window_end,
    symbol,
    'traded_volume_1m' AS metric_name,
    CAST(COALESCE(SUM(volume), 0) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE crypto_ticks_source, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, symbol

  UNION ALL

  SELECT
    window_start,
    window_end,
    symbol,
    'traded_notional_1m' AS metric_name,
    CAST(COALESCE(SUM(trade_notional), 0) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE crypto_ticks_source, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, symbol

  UNION ALL

  SELECT
    window_start,
    window_end,
    symbol,
    'vwap_1m' AS metric_name,
    CAST(
      CASE
        WHEN SUM(volume) = 0 THEN 0.0
        ELSE SUM(trade_notional) / SUM(volume)
      END AS DOUBLE
    ) AS metric_value
  FROM TABLE(TUMBLE(TABLE crypto_ticks_source, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, symbol

  UNION ALL

  SELECT
    window_start,
    window_end,
    symbol,
    'price_volatility_1m' AS metric_name,
    CAST(MAX(price) - MIN(price) AS DOUBLE) AS metric_value
  FROM TABLE(TUMBLE(TABLE crypto_ticks_source, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, symbol
);

END;
