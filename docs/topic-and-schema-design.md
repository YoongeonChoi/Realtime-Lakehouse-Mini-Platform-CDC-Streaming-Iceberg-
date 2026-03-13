# Topic And Schema Design

## Topic Naming

- `raw.cdc.commerce.public.orders`
- `raw.cdc.commerce.public.payments`
- `raw.cdc.commerce.public.refunds`
- `raw.event.commerce.user_behavior_v3`

## Keying Strategy

- CDC 키: 테이블 PK 기반
- 앱 이벤트 키: `user_id`

## Partitioning

- 로컬 데모 기준: 3 partitions
- 실제 운영에서는 처리량, consumer lag, broker 리소스를 보고 재산정

## Schema Policy

- CDC: Debezium unwrap JSON
- App events: Avro + Schema Registry
- 글로벌 호환성: `BACKWARD_TRANSITIVE`

## Evolution Scenario

대표 예시는 `orders.coupon_id` nullable 필드 추가입니다.
