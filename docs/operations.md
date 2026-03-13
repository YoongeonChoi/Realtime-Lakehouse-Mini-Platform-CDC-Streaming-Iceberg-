# Operations And Incident Playbook

## Startup

1. `.\scripts\bootstrap.ps1`
2. `.\scripts\seed-postgres.ps1`
3. `.\scripts\produce-events.ps1`
4. `.\scripts\run-dq.ps1`

## Recovery Patterns

### Debezium Connector Broken

- `.\scripts\register-connector.ps1` 재실행
- 필요 시 커넥터 삭제 후 동일 이름으로 재생성

### Flink Job Failed

- `docker compose logs flink-jobmanager`
- `.\scripts\run-flink-sql.ps1`로 재배포

### Schema Change

- App event는 Schema Registry compatibility 유지
- Iceberg는 nullable add-column 위주로 운영
- breaking change는 새 topic 또는 새 gold metric으로 분리
