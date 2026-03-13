# Performance Template

## Benchmark Plan

- 입력 1: Postgres order / payment / refund burst 500 rows
- 입력 2: user_behavior event 5,000 rows
- 관찰치: Kafka lag, Flink latency, Trino freshness, container CPU / memory

## Measurement Commands

```powershell
.\scripts\seed-postgres.ps1
.\scripts\produce-events.ps1 -Count 5000 -IntervalMs 20
docker stats --no-stream
```
