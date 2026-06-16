# daily-transaction-batch

> Spring Batch를 활용해 은행의 일일 대용량 거래 데이터를 **읽고(Read) → 처리하고(Process) → 저장하는(Write)** 청크 기반 배치 구조를 구현하고 학습하기 위한 프로젝트입니다.

## 📌 프로젝트 목표

- Spring Batch의 핵심 개념(`Job`, `Step`, `Chunk`, `ItemReader/Processor/Writer`)을 직접 구현하며 이해한다.
- 대용량 거래 데이터를 메모리 부담 없이 처리하는 **청크 지향(Chunk-Oriented) 처리 모델**을 학습한다.
- 장애 상황에 대비한 **내결함성(Fault Tolerance: Skip / Retry)** 과 **재시작(Restart)** 메커니즘을 익힌다.
- 처리량을 높이기 위한 **확장(Scaling) 전략**(멀티스레드 Step, 파티셔닝)을 단계적으로 적용한다.

## 🧱 기술 스택

| 구분 | 내용 |
|------|------|
| Language | Java 17 |
| Framework | Spring Boot 3.5.7, Spring Batch 5.x |
| Persistence | Spring Data JPA, Hibernate |
| Database | H2 (인메모리, 학습용) |
| Build | Gradle |
| Etc | Lombok |

## 🗂️ 도메인 개요

학습용 데이터는 `src/main/resources/transactions.csv` 에 위치합니다.

```csv
id,user_id,amount,created_at
1,101,5000,2025-11-01
2,102,3000,2025-11-01
...
```

| 컬럼 | 설명 |
|------|------|
| `id` | 거래 고유 식별자 |
| `user_id` | 거래를 발생시킨 사용자 ID |
| `amount` | 거래 금액 |
| `created_at` | 거래 발생 일자 |

## ⚙️ 배치 처리 흐름

Spring Batch의 **Chunk-Oriented Processing** 모델을 사용합니다. ItemReader가 데이터를 하나씩 읽어 청크(chunk) 크기만큼 모은 뒤, 한 트랜잭션 단위로 ItemWriter가 일괄 저장합니다.

```
[transactions.csv]
        │  read (FlatFileItemReader)
        ▼
   TransactionInput  ──process (ItemProcessor: 검증/변환)──▶  Transaction
        │
        ▼  write (JpaItemWriter, chunk 단위 트랜잭션 커밋)
   [H2 Database]
```

- **Reader**: `FlatFileItemReader` 로 CSV를 한 행씩 읽어 들임
- **Processor**: 금액 검증, 잘못된 데이터 필터링/변환 등 비즈니스 로직 수행
- **Writer**: `JpaItemWriter` 로 청크 단위 일괄 저장
- **Fault Tolerance**: 파싱 오류·검증 실패는 `skip`, 일시적 DB 오류는 `retry`

## 🚀 실행 방법

```bash
# 빌드
./gradlew build

# 배치 실행 (Spring Boot 가 등록된 Job 을 자동 실행)
./gradlew bootRun

# 특정 Job 파라미터 전달 예시
./gradlew bootRun --args='--spring.batch.job.name=dailyTransactionJob date=2025-11-01'
```

> H2 콘솔: 애플리케이션 실행 후 `http://localhost:8080/h2-console` (설정 시)

## 📁 프로젝트 구조 (예정)

```
src/main/java/com/example/dtb
├── DtbApplication.java
├── domain/            # JPA 엔티티 (Transaction 등)
├── job/               # Job / Step 설정
├── reader/            # ItemReader 구성
├── processor/         # ItemProcessor 구성
└── writer/            # ItemWriter 구성
```

## 🎓 학습 단계 (로드맵)

1. **기본 청크 배치** — CSV → 검증/변환 → DB 저장
2. **내결함성** — Skip / Retry / SkipListener 적용
3. **재시작 & 멱등성** — JobRepository 기반 재시작, 중복 처리 방지
4. **확장(Scaling)** — 멀티스레드 Step → 파티셔닝으로 처리량 향상 ✅
5. **집계 Step** — 사용자/일자별 거래 집계 Step 추가

---

## 📐 Phase 4: 확장(Scaling)

처리량을 높이기 위한 두 가지 전략을 단계적으로 적용합니다.

### Part 1 — 멀티스레드 Step (`multiThreadedTransactionJob`)

여러 스레드가 **하나의 Reader를 공유**하며 청크를 병렬 처리합니다.

```
[transactions.csv]
     │
     ▼  SynchronizedItemStreamReader (read()를 직렬화 — thread-safe)
     │
     ├─ Thread-1 ──▶ chunk(5) ──▶ Processor ──▶ Writer
     ├─ Thread-2 ──▶ chunk(5) ──▶ Processor ──▶ Writer
     ├─ Thread-3 ──▶ chunk(5) ──▶ Processor ──▶ Writer
     └─ Thread-4 ──▶ chunk(5) ──▶ Processor ──▶ Writer
```

- `FlatFileItemReader`는 내부 상태(현재 라인 포지션)를 가지므로 thread-safe하지 않다.
- `SynchronizedItemStreamReader`로 감싸면 `read()` 호출이 직렬화되어 안전하게 공유 가능.
- `ThreadPoolTaskExecutor` (corePoolSize=4) → Step에 `.taskExecutor()` 설정.

### Part 2 — 파티셔닝 (`partitionedTransactionJob`)

데이터를 **N개의 독립 파티션으로 분할**하고 각 파티션이 전용 Reader를 가집니다.

```
partitionedTransactionStep (master)
  ├─ partitionWorkerStep:partition0  ── rows  1~5  (linesToSkip=1,  lineCount=5)
  ├─ partitionWorkerStep:partition1  ── rows  6~10 (linesToSkip=6,  lineCount=5)
  ├─ partitionWorkerStep:partition2  ── rows 11~15 (linesToSkip=11, lineCount=5)
  ├─ partitionWorkerStep:partition3  ── rows 16~20 (linesToSkip=16, lineCount=5)
  └─ partitionWorkerStep:partition4  ── rows 21~25 (linesToSkip=21, lineCount=5)
```

- `TransactionLineRangePartitioner`: 25행을 5개 라인 범위로 분할, `linesToSkip`/`lineCount`를 ExecutionContext에 저장.
- `@StepScope` Reader: 파티션 실행 시점에 stepExecutionContext 값을 주입받아 각 파티션 전용 인스턴스 생성 → Reader 동기화 불필요.
- `TaskExecutorPartitionHandler`: `batchTaskExecutor`로 워커 Step을 병렬 실행.

### 멀티스레드 Step vs 파티셔닝 비교

| 구분 | 멀티스레드 Step | 파티셔닝 |
|------|----------------|----------|
| Reader | 1개 공유 (동기화 필요) | 파티션마다 독립 인스턴스 (`@StepScope`) |
| 분할 단위 | 청크 | 데이터 범위(파티션) |
| 진행 상황 추적 | Step 1개의 카운터 | 워커 Step별 독립 실행 이력 |
| 재시작 | 마지막 커밋 지점부터 | 실패한 파티션만 재시작 가능 |


## 📚 참고

- [Spring Batch Reference (공식 문서)](https://docs.spring.io/spring-batch/reference/)
- [Spring Boot Batch 연동](https://docs.spring.io/spring-boot/reference/howto/batch.html)
