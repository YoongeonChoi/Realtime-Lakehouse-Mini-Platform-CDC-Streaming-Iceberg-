# Demo Script

## 3 To 5 Minute Flow

### 1. Problem Setup

- 이 프로젝트는 Postgres OLTP 변경과 사용자 행동 이벤트를 하나의 실시간 레이크하우스로 수렴시키는 데모입니다.
- 목표는 Kafka, Flink, Iceberg, Trino, Superset, Great Expectations를 하나의 스토리로 보여주는 것입니다.

### 2. CDC And Streaming Ingestion

- `orders`, `payments`, `refunds`는 Postgres에 적재됩니다.
- Debezium이 WAL 기반 변경을 읽어 Kafka `raw.cdc.commerce.public.*` 토픽으로 발행합니다.
- 사용자 행동 이벤트는 Schema Registry 기반 Avro로 `raw.event.commerce.user_behavior_v3`에 적재됩니다.

### 3. Lakehouse Processing

- Flink SQL이 Kafka source를 읽어 bronze / silver / gold Iceberg 테이블에 적재합니다.
- bronze는 원본 보존, silver는 표준화 이벤트, gold는 1분 KPI 집계 계층입니다.

### 4. Query And Dashboard

- Trino에서 `gold.commerce_kpis_1m`를 조회해 `orders_created`, `gross_order_value`, `payments_succeeded`, `refund_amount`를 확인합니다.
- Superset에서는 동일 테이블을 읽어 KPI 차트를 구성합니다.

### 5. Data Quality

- Great Expectations는 `gold.commerce_kpis_1m`에 대해 row count, null 여부, metric_name 유효성, metric_value 범위를 검증합니다.
- 결과는 Data Docs HTML로 생성됩니다.

## Live Commands

```powershell
.\scripts\bootstrap.ps1
.\scripts\seed-postgres.ps1
.\scripts\produce-events.ps1 -Count 120 -IntervalMs 250
.\scripts\run-dq.ps1
```

```sql
SELECT *
FROM iceberg.gold.commerce_kpis_1m
ORDER BY window_start DESC, metric_name
LIMIT 20;
```

## What To Emphasize

- CDC와 이벤트 스트리밍이 같은 Iceberg 저장 계층으로 수렴된다는 점
- Flink checkpoint와 Iceberg commit을 통해 장애 복구 스토리를 만들 수 있다는 점
- 단순 적재가 아니라 medallion, schema evolution, DQ, query serving까지 이어진다는 점

