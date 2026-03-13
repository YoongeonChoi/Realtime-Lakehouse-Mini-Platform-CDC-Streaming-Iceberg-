# Portfolio Playbook

## One-Line Summary

PostgreSQL CDC와 애플리케이션 이벤트를 Kafka로 통합하고, Flink 스트리밍 처리로 Iceberg 레이크하우스에 적재한 뒤 Trino, Superset, Great Expectations까지 연결한 end-to-end 실시간 데이터 플랫폼입니다.

## What This Project Demonstrates

- CDC 파이프라인 설계와 Debezium 운영 이해
- Kafka topic / schema / compatibility 설계
- Flink SQL 기반 스트리밍 정제와 윈도우 집계
- Iceberg bronze / silver / gold 모델링
- Trino 기반 서빙과 time-travel 조회
- Great Expectations 기반 데이터 품질 검증
- 장애 분석, 복구, 재처리, 운영 문서화 능력

## Architecture Explanation

### Source Layer

- Postgres는 주문, 결제, 환불을 저장하는 OLTP 역할을 합니다.
- Debezium PostgreSQL Connector가 WAL을 읽어 변경 이벤트를 Kafka로 밀어냅니다.
- 앱 이벤트는 producer가 Avro + Schema Registry 조합으로 직접 Kafka에 적재합니다.

### Streaming Layer

- Kafka는 CDC와 행동 이벤트를 분리된 토픽으로 보관합니다.
- Flink는 Kafka source를 읽어 원본 보존, 표준화, 집계의 세 단계를 만듭니다.

### Lakehouse Layer

- Iceberg는 테이블 메타데이터와 snapshot 기반으로 상태를 관리합니다.
- object storage는 MinIO를 사용했고, REST catalog를 통해 Flink와 Trino가 같은 테이블을 바라봅니다.

### Serving Layer

- Trino는 분석용 SQL 인터페이스입니다.
- Superset은 KPI 시각화 역할을 합니다.
- Great Expectations는 gold 테이블 품질 검증을 자동화합니다.

## Why These Technologies

### Why Debezium

- Postgres CDC를 애플리케이션 수정 없이 데모할 수 있습니다.
- 테이블 단위 CDC 토픽이 명확해 topic 설계 설명이 쉽습니다.

### Why Kafka

- CDC와 앱 이벤트를 같은 이벤트 버스에 올려 표준화하기 좋습니다.
- consumer group, partition, ordering, replay 개념을 함께 설명할 수 있습니다.

### Why Flink SQL

- 스트리밍 조인과 윈도우 집계를 SQL 중심으로 표현할 수 있습니다.
- checkpoint 기반 장애 복구와 exactly-once 이야기를 하기 좋습니다.

### Why Iceberg

- 스키마 진화, snapshot, time travel, compaction 같은 lakehouse 핵심 개념을 보여주기 좋습니다.
- 배치와 스트리밍을 같은 테이블 포맷으로 수렴시킬 수 있습니다.

### Why Trino And Superset

- SQL 조회와 대시보드 연결이 빠르고 설명이 직관적입니다.
- "적재"에서 끝나지 않고 "즉시 활용"까지 보여줄 수 있습니다.

## Hard Problems And How I Solved Them

### 1. Debezium Timestamp Format Mismatch

문제:

- Debezium unwrap JSON을 Flink SQL에서 읽을 때 CDC timestamp가 `TIMESTAMP`가 아니라 epoch milliseconds 형태로 들어왔습니다.

영향:

- Flink source DDL이 실제 이벤트 포맷과 맞지 않아 파싱과 변환이 깨졌습니다.

해결:

- source 컬럼을 `BIGINT`로 받고 `TO_TIMESTAMP_LTZ(..., 3)`로 변환했습니다.
- `created_at`, `updated_at`, `__source_ts_ms`를 기준으로 event_time을 계산하도록 정리했습니다.

배운 점:

- CDC 메시지는 커넥터 문서상의 개념과 실제 unwrap 이후 payload 형태를 구분해서 다뤄야 합니다.

### 2. Java 17 Reflection And Checkpoint Serialization Failure

문제:

- Flink checkpoint 중 Iceberg sink가 Kryo로 `HeapByteBuffer`를 직렬화할 때 `InaccessibleObjectException`이 발생했습니다.

원인:

- Java 17 모듈 시스템 때문에 `java.util`, `java.nio` 내부 필드에 reflection 접근이 차단되었습니다.

해결:

- Flink JVM에 `--add-opens=java.base/java.util=ALL-UNNAMED`
- `--add-opens=java.base/java.nio=ALL-UNNAMED`
- 를 주입해 checkpoint serialization 경로를 열었습니다.
- 단순 설정 파일만으로는 누락될 수 있어 runtime 환경변수로도 강제 적용했습니다.

배운 점:

- "설정을 추가했다"와 "실행 중 JVM이 실제로 그 옵션을 먹었다"는 별개의 문제입니다.
- 컨테이너 내부 프로세스 인자를 직접 확인해야 합니다.

### 3. Iceberg Concurrent Commit Conflict

문제:

- `silver.order_events`에 대해 `orders`, `payments`, `refunds` 세 개의 `INSERT INTO`가 동시에 같은 테이블로 쓰면서 REST catalog commit 충돌이 발생했습니다.

증상:

- `CommitStateUnknownException`
- `Service failed: 500: Unknown failure`

해결:

- 세 개의 sink를 각각 두지 않고, `UNION ALL` 기반 단일 `INSERT INTO silver.order_events`로 합쳤습니다.

배운 점:

- Iceberg는 같은 테이블에 대한 동시 sink 경합을 무조건 안전하게 추상화해주지 않습니다.
- 스트리밍 파이프라인에서는 sink fan-in 구조를 먼저 의심해야 합니다.

### 4. Gold Window Not Closing

문제:

- `gold.commerce_kpis_1m`가 비어 있었고, bronze / silver는 적재되는데 1분 KPI만 생성되지 않았습니다.

원인:

- 로컬 Kafka 토픽은 single partition인데 Flink source parallelism이 2라서, idle subtask가 watermark를 막고 있었습니다.

해결:

- `SET 'table.exec.source.idle-timeout' = '10 s';`를 추가해 idle source가 watermark 진행을 막지 않게 했습니다.

배운 점:

- 로컬 데모 환경에서는 "낮은 입력량 + 적은 partition"이 오히려 윈도우 집계 검증을 더 어렵게 만들 수 있습니다.

## Operational Thinking

이 프로젝트에서 강조할 운영 관점:

- Debezium connector 재등록과 CDC 재시작 절차
- Flink job 재배포와 checkpoint 기반 복구
- Iceberg warehouse 청소 후 catalog 재기동 같은 로컬 재처리 시나리오
- schema evolution은 nullable add-column 중심으로 진행
- gold 테이블은 DQ를 걸어 대시보드 앞단에서 품질을 확인

## Interview Answers You Can Reuse

### "이 프로젝트에서 가장 어려웠던 점은?"

가장 어려웠던 지점은 Flink와 Iceberg sink가 체크포인트 시점에 Java 17 모듈 제한과 Iceberg commit 충돌 때문에 계속 재시작되던 문제였습니다. 처음에는 단순 직렬화 버그처럼 보였지만, 실제로는 JVM reflection 제한과 동일 Iceberg 테이블에 대한 동시 sink 경합이 겹쳐 있었습니다. JVM 옵션을 실제 런타임에 강제 주입하고, silver sink를 단일 `UNION ALL` 구조로 바꿔서 안정화했습니다.

### "이 프로젝트에서 기술적으로 가장 설명하고 싶은 부분은?"

CDC와 이벤트 스트리밍을 같은 Iceberg 저장 계층으로 수렴시켰다는 점입니다. Postgres 변경은 Debezium으로, 앱 이벤트는 Schema Registry 기반 Avro로 Kafka에 넣고, Flink SQL이 이를 bronze / silver / gold로 정제합니다. 이렇게 하면 배치와 스트리밍이 같은 테이블 포맷 위에서 만날 수 있고, Trino와 Superset은 같은 Iceberg 테이블을 바로 조회할 수 있습니다.

### "왜 이 조합을 선택했나요?"

면접에서 자주 나오는 핵심 키워드를 억지로 나열하는 대신, 실제로 함께 동작하는 구조를 만들고 싶었습니다. Debezium은 CDC를, Kafka는 이벤트 버스를, Flink는 실시간 처리와 복구를, Iceberg는 lakehouse 저장 계층을, Trino와 Superset은 활용 계층을 맡습니다. 각 기술이 역할이 명확해서 전체 스토리를 설명하기 좋습니다.

## Resume Bullet Examples

- Built a local reproducible real-time lakehouse platform integrating PostgreSQL CDC, Kafka, Flink SQL, Iceberg, Trino, Superset, and Great Expectations.
- Designed Kafka topics, schema evolution strategy, and medallion-model Iceberg tables for commerce CDC and user behavior streaming data.
- Diagnosed and resolved Flink checkpoint failures on Java 17 and Iceberg concurrent commit conflicts to stabilize end-to-end streaming ingestion.
- Implemented near-real-time KPI aggregation and data quality checks with reproducible demo flows for portfolio and interview presentations.

