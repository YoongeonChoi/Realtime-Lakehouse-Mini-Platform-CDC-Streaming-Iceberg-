# Operations And Incident Playbook

## Startup

1. `.\scripts\bootstrap.ps1`
2. `.\scripts\seed-postgres.ps1`
3. `.\scripts\produce-events.ps1`
4. `.\scripts\produce-crypto-ticks.ps1`
5. `.\scripts\run-dq.ps1`

참고:

- `produce-crypto-ticks.ps1`는 기본적으로 `EventTimeStepMs=250`을 사용해 짧은 실행으로도 `gold.crypto_market_kpis_1m` 적재를 확인할 수 있습니다.

## Recovery Patterns

### Debezium Connector Broken

- `.\scripts\register-connector.ps1` 재실행
- 필요 시 커넥터 삭제 후 동일 이름으로 재생성

### Kafka Down

- 증상:
  - CDC / 이벤트 ingestion 정지
  - Kafka Connect와 Flink source lag 증가
- 확인:
  - `docker compose ps`
  - `docker compose logs kafka --tail=200`
  - `docker compose logs connect --tail=200`
- 대응:
  - Kafka 컨테이너 복구
  - Kafka Connect / Flink source 정상 재연결 확인

### Flink Job Failed

- `docker compose logs flink-jobmanager`
- `.\scripts\run-flink-sql.ps1`로 재배포

### Flink Restart / Checkpoint Recovery

- 확인:
  - Flink UI에서 job state 확인
  - 최근 checkpoint completed 여부 확인
- 대응:
  - checkpoint 복구 상태 점검
  - 필요 시 job 재배포
  - 동일 Iceberg sink 경합 여부 확인

### Schema Change

- App event는 Schema Registry compatibility 유지
- Iceberg는 nullable add-column 위주로 운영
- breaking change는 새 topic 또는 새 gold metric으로 분리

### Late Event

- event time과 watermark를 사용해 집계합니다.
- 로컬 환경에서는 idle source가 watermark를 막을 수 있어 `table.exec.source.idle-timeout`을 적용했습니다.

### Backfill

- warehouse 초기화 후 Kafka source를 earliest-offset부터 다시 읽게 해 재처리 흐름을 검증할 수 있습니다.
- 운영 환경에서는 재처리 범위를 분리해 topic replay 또는 별도 batch backfill로 수행합니다.
