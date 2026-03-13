SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

SELECT *
FROM iceberg.bronze.orders_cdc
ORDER BY cdc_source_ts DESC
LIMIT 20;

SELECT *
FROM iceberg.gold.commerce_kpis_1m
ORDER BY window_start DESC, metric_name
LIMIT 50;

SELECT *
FROM iceberg.gold.behavior_kpis_1m
ORDER BY window_start DESC, metric_name
LIMIT 50;

SELECT *
FROM iceberg.gold."commerce_kpis_1m$snapshots"
ORDER BY committed_at DESC;

SELECT *
FROM iceberg.gold.commerce_kpis_1m
FOR TIMESTAMP AS OF TIMESTAMP '2026-03-13 12:00:00 Asia/Seoul'
ORDER BY window_start DESC, metric_name
LIMIT 50;
