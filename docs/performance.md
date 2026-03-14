# Performance Notes

## Benchmark Plan

- 입력 1: Postgres order / payment / refund burst 500 rows
- 입력 2: user_behavior event 5,000 rows
- 입력 3: crypto tick event 100,000 ~ 100,000,000 rows
- 관찰치: Kafka lag, Flink latency, Trino freshness, container CPU / memory

## Smoke Test Observations

아래 수치는 기능 검증을 위한 로컬 smoke test 기준이며, 정식 부하 테스트 수치가 아닙니다.

- Flink checkpoint가 반복 성공하는 상태를 확인했습니다.
- 관찰된 checkpoint duration 예시: 약 `24 ms ~ 84 ms`
- 검증 시점 적재 결과:
  - `bronze.orders_cdc = 35`
  - `bronze.payments_cdc = 18`
  - `bronze.refunds_cdc = 9`
  - `silver.order_events = 62`
  - `gold.commerce_kpis_1m = 32`
  - `bronze.crypto_ticks = 800`
  - `silver.crypto_ticks_1s = 529`
  - `gold.crypto_market_kpis_1m = 60`

예시 KPI:

- `orders_created = 4`
- `gross_order_value = 669.8`
- `payments_succeeded = 2`
- `refund_amount = 89.9`

crypto market KPI 예시:

- `tick_count_1m`
- `traded_volume_1m`
- `traded_notional_1m`
- `vwap_1m`
- `price_volatility_1m`

crypto smoke run 메모:

- `.\scripts\produce-crypto-ticks.ps1 -Count 600 -IntervalMs 10`
- 기본 `EventTimeStepMs=250`로 약 6초 동안 150초 분량의 event time을 생성해 1분 window를 빠르게 닫도록 구성

## Tuning Notes

- 로컬 Kafka 토픽이 단일 partition일 때 Flink 병렬 source 일부가 idle 상태가 되어 watermark 전진이 막힐 수 있었습니다.
- 이를 해결하기 위해 `table.exec.source.idle-timeout`을 설정해 gold 윈도우 집계가 닫히도록 조정했습니다.
- 동일 Iceberg 테이블에 대한 복수 sink는 commit 충돌을 유발할 수 있어 단일 sink fan-in 구조로 정리했습니다.
- crypto tick 버전은 대량 스트리밍 / event-time / window aggregation 설명에 적합하도록 symbol 단위 집계를 추가했습니다.

## Scale-Up Scenario

crypto tick 버전의 확장 목표:

- 데이터셋: simulated crypto tick feed 또는 실제 crypto tick dataset
- 목표 크기: `100M rows`
- 목표 처리량: `500+ events/sec`
- 목표 dashboard freshness: `2s ~ 5s`

확장 시 튜닝 포인트:

- Kafka partition 증가
- Flink parallelism 증가
- Iceberg compaction 주기 조정
- Trino query tuning
- object storage I/O 최적화

## Measurement Commands

```powershell
.\scripts\seed-postgres.ps1
.\scripts\produce-events.ps1 -Count 5000 -IntervalMs 20
.\scripts\produce-crypto-ticks.ps1 -Count 100000 -IntervalMs 1 -EventTimeStepMs 50
docker stats --no-stream
```
