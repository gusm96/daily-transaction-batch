# 작업 계획서 — Spring Batch 대용량 거래 데이터 처리 구조 구현

- **문서 ID**: 260615_spring-batch-transaction-design
- **작성일**: 2026-06-15
- **작성자**: Seongmo Gu
- **대상 프로젝트**: daily-transaction-batch
- **목적**: 은행 일일 대용량 거래 데이터를 청크 기반으로 처리하는 Spring Batch 구조를 단계적으로 구현·학습한다.

> 본 계획서는 Context7를 통해 조회한 Spring Batch 5.2.x 공식 레퍼런스를 기반으로 작성되었습니다.

### 개정 이력
| 버전 | 일자 | 내용 |
|------|------|------|
| v1 | 2026-06-15 | 최초 작성 |
| v2 | 2026-06-15 | Codex(GPT-5.5) 리뷰 반영 — 도메인 타입/멱등성/H2 재시작/JobParameters/청크 크기/집계 엔티티 등 Critical·Major 항목 반영 |

---

## 1. 배경 및 목표

### 1.1 배경
은행 거래 데이터는 일 단위로 대량 발생하며, 이를 안정적으로 적재·검증·집계하는 배치 처리가 필요하다. Spring Batch는 청크 지향 처리 모델을 통해 대용량 데이터를 **자동 트랜잭션 관리, 재시작, Skip/Retry 기반 내결함성**으로 안전하게 처리한다.

### 1.2 목표
- [ ] CSV 거래 데이터를 읽어 검증·변환 후 DB에 저장하는 기본 배치 완성
- [ ] Skip/Retry를 통한 내결함성 확보
- [ ] JobRepository 기반 재시작 및 멱등성 보장
- [ ] 멀티스레드 Step → 파티셔닝으로 처리량 확장
- [ ] 사용자/일자별 집계 Step 추가

### 1.3 범위
- **포함**: Job/Step 설계, Reader/Processor/Writer 구현, 내결함성, 확장 전략, 단위/통합 테스트
- **제외**: 실제 운영 DB 연동(H2로 학습), 스케줄러 인프라(Quartz/Airflow 등), 모니터링 대시보드

### 1.4 작업 방식 (Git 브랜치 전략)
각 Phase의 DoD 달성 시점을 명확히 기록하고, 설계 변경 시 이전 단계로 롤백 가능하도록 Phase별 브랜치를 사용한다.

```
phase/1-basic-chunk → phase/2-fault-tolerance → phase/3-restart-idempotency
→ phase/4-scaling → phase/5-aggregation
```
각 Phase 완료 시 DoD를 커밋 메시지에 명시하고 main에 병합한다.

---

## 2. 현황 분석

| 항목 | 현재 상태 |
|------|-----------|
| 빌드 | `spring-boot-starter-batch`, `spring-boot-starter-data-jpa`, `h2`, `lombok` 의존성 존재 |
| 진입점 | `DtbApplication.java` (기본 Spring Boot 앱, 배치 설정 없음) |
| 데이터 | `src/main/resources/transactions.csv` (id, user_id, amount, created_at / 25건, 일자 2025-11-01~11-05) |
| 설정 | `application.properties` 에 앱 이름만 정의됨 |

➡ **결론**: 배치 관련 코드는 아직 없으며, 의존성과 데이터만 준비된 초기 상태. 본 계획서에 따라 단계적으로 구현한다.

---

## 3. 도메인 설계

### 3.1 입력 모델 (`TransactionInput`)
CSV 한 행을 매핑하는 DTO. 원시 문자열을 받아 Processor에서 검증·변환한다.

| 필드 | 타입 | 비고 |
|------|------|------|
| id | Long | 거래 식별자 |
| userId | Long | `user_id` 매핑 (카멜케이스 변환) |
| amount | BigDecimal | 거래 금액 |
| createdAt | LocalDate | `created_at` 매핑 |

### 3.2 영속 엔티티 (`Transaction`)
JPA `@Entity`. Processor를 통과한 유효 거래만 저장.

| 필드 | 타입 | 제약 |
|------|------|------|
| id | Long (@Id) | CSV의 id 사용 (멱등성 키) |
| userId | Long | not null |
| amount | BigDecimal(19,2) | `>= 0` |
| createdAt | LocalDate | not null |

> **[금액 타입 — C-1 반영]** 금융 도메인 기본 요건에 따라 `amount`는 `BigDecimal`을 사용한다. `Long`(원화 정수)으로 한정하면 소수점 포함 데이터 유입 시 파싱 단계에서 무조건 실패하므로, 확장성을 고려해 처음부터 `BigDecimal(precision=19, scale=2)`로 설계한다.

> **[멱등성 — C-2 반영]** `id`를 PK로 사용한다. `JpaItemWriter`는 내부적으로 `EntityManager.merge()`를 호출하므로, **같은 id가 존재하면 UPDATE, 없으면 INSERT** 된다. 즉 멱등성은 Phase 1 구현 시점부터 자동으로 보장된다. Phase 3에서는 이 동작이 실제로 멱등하게 작동하는지 **검증**하는 것이 목표다(별도 존재 검사 로직 추가는 원칙적으로 불필요).

### 3.3 집계 엔티티 (`UserDailySummary`) — Phase 5 대상
사용자/일자별 거래 합계를 저장한다. (M-3 반영 — 누락 보완)

| 필드 | 타입 | 제약 |
|------|------|------|
| id | Long (@Id, 생성 전략) | 대리 키 |
| userId | Long | not null, 복합 유니크 키 |
| transactionDate | LocalDate | not null, 복합 유니크 키 |
| totalAmount | BigDecimal(19,2) | 일자별 거래 금액 합계 |
| transactionCount | Integer | 일자별 거래 건수 |

> 복합 유니크 제약: `(userId, transactionDate)` — 집계 Step 재실행 시 중복 행 방지(멱등).

---

## 4. 작업 단계 (Phase)

### Phase 1 — 기본 청크 배치 구축
**목표**: CSV → 검증/변환 → H2 저장하는 단일 Step Job 완성

- [ ] `application.properties` 배치 설정 추가
  - `spring.batch.jdbc.initialize-schema=always` (메타데이터 테이블 자동 생성)
  - `spring.jpa.hibernate.ddl-auto=create-drop` (Phase 1~2 한정 — **Phase 3 진입 전 변경 필요**, 6절·C-3 참고)
  - H2 콘솔 활성화
  - **[m-1 반영]** `spring.batch.job.enabled`는 Spring Boot 3.x에서 **기본값 `true`** 이므로 명시 불필요. 단, Phase 5/테스트 단계에서 `false`로 전환해 `JobLauncher.run()`을 직접 호출하는 방식을 학습 항목으로 둔다.
- [ ] **[M-4 반영] JobParameters 설계**: Spring Batch는 동일 파라미터로 COMPLETED된 Job을 재실행하지 않는다(`JobInstanceAlreadyCompleteException`). Phase 1부터 매 실행 유니크 파라미터(`run.id` 또는 `date`)를 전달해 반복 실행 가능하게 한다.
- [ ] `Transaction` 엔티티, `TransactionInput` DTO 작성 (amount: `BigDecimal`)
- [ ] **[S-2 반영]** `FlatFileItemReader<TransactionInput>` 구성
  - `linesToSkip(1)` (헤더 제외)
  - `DelimitedLineTokenizer` 구분자 `,`, `names`를 CSV 컬럼 순서(`id,user_id,amount,created_at`)와 일치시키고 `BeanWrapperFieldSetMapper`로 `user_id → userId` 매핑 처리
- [ ] `ItemProcessor<TransactionInput, Transaction>` — 금액 음수/형식 오류 검증·변환
- [ ] `JpaItemWriter<Transaction>` 구성
- [ ] `JobBuilder` / `StepBuilder` 로 Job·Step 구성
- [ ] **[M-5 반영]** 청크 크기를 **5 또는 10**으로 설정한다. 데이터가 25건인데 `chunk(100)`이면 청크가 한 번도 분할되지 않아 "청크 단위 커밋/롤백" 학습 효과가 사라진다.

**참고 코드 (공식 문서 기준 — 청크 Step 구성)**:
```java
@Bean
public Step transactionStep(JobRepository jobRepository,
                            PlatformTransactionManager transactionManager,
                            ItemReader<TransactionInput> reader,
                            ItemProcessor<TransactionInput, Transaction> processor,
                            ItemWriter<Transaction> writer) {
    return new StepBuilder("transactionStep", jobRepository)
        .<TransactionInput, Transaction>chunk(10, transactionManager) // 학습용: 데이터보다 작게
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .build();
}

@Bean
public Job dailyTransactionJob(JobRepository jobRepository, Step transactionStep) {
    return new JobBuilder("dailyTransactionJob", jobRepository)
        .start(transactionStep)
        .build();
}
```

**완료 기준(DoD)**:
- `bootRun` 실행 시 25건이 H2에 저장되고, 잘못된 행은 저장되지 않음
- 유니크 파라미터로 2회 이상 반복 실행 가능
- H2 콘솔에서 청크 단위(5~10건)로 커밋이 발생함을 확인

---

### Phase 2 — 내결함성 (Fault Tolerance)
**목표**: 일부 데이터 오류가 전체 Job을 실패시키지 않도록 Skip/Retry 적용

- [ ] `.faultTolerant()` 활성화
- [ ] **[S-1 반영]** 파싱 오류(`FlatFileParseException`), 검증 오류 → `skip`
  - 검증 예외는 Spring Batch 표준 `org.springframework.batch.item.validator.ValidationException` 또는 Processor에서 던지는 **커스텀 예외**를 사용한다(자바 표준 `jakarta.validation.ValidationException`과 혼동 주의). 사용할 예외 클래스명을 코드에 명시한다.
- [ ] **[M-2 반영] skipLimit 기준 설정**: 임의값 대신 근거를 기록한다. 예) "전체 건수의 N% 이하". 학습 단계에서는 작게(예: `skipLimit(3)`) 설정하고, **skipLimit 초과 시 Job이 FAILED로 전환됨**을 함께 검증한다.
- [ ] 일시적 DB 오류(`DeadlockLoserDataAccessException` 등) → `retry`, `retryLimit` 설정
- [ ] **[m-4 반영]** `SkipListener`를 `@Component`로 Bean 등록 후 주입해 스킵 항목을 로깅한다(인스턴스 직접 생성 시 `@Autowired` 의존성이 null이 됨).

**참고 코드 (공식 문서 기준)**:
```java
return new StepBuilder("transactionStep", jobRepository)
    .<TransactionInput, Transaction>chunk(10, transactionManager)
    .reader(reader).processor(processor).writer(writer)
    .faultTolerant()
    .skip(InvalidTransactionException.class) // 커스텀 검증 예외
    .skip(FlatFileParseException.class)
    .skipLimit(3)                            // 근거: 학습용 소량 기준
    .retry(DeadlockLoserDataAccessException.class)
    .retryLimit(3)
    .listener(transactionSkipListener)       // @Component 주입
    .build();
```

**DoD**:
- 의도적으로 오류 행을 섞은 CSV로 실행 시, 정상 행은 저장되고 오류 행은 스킵·로깅됨
- 오류 행이 skipLimit를 초과하면 Job이 FAILED로 전환됨을 확인

---

### Phase 3 — 재시작 & 멱등성
**목표**: 중단된 Job을 안전하게 재시작하고, 재실행 시 중복 적재가 없도록 보장

> **[진입 선행 조건 — C-3 반영]** Phase 3 시작 전 반드시 H2를 **파일 모드**로 전환한다. 인메모리 + `create-drop`은 앱 종료 시 JobRepository 메타데이터까지 삭제되어 재시작이 새 실행으로 인식된다.
> - `spring.datasource.url=jdbc:h2:file:./data/batchdb`
> - `spring.jpa.hibernate.ddl-auto=update`

- [ ] H2 파일 모드 전환 및 메타데이터 영속 확인
- [ ] `JobParameters`(예: `date`) 기반 실행으로 동일 파라미터 재시작 동작 확인
- [ ] 실패 지점부터 재시작되는지 검증 (JobRepository 메타데이터)
- [ ] **[C-2 연계]** `JpaItemWriter`의 `merge()` 기반 멱등성이 실제로 작동하는지 검증(동일 Job 2회 실행 후 행 수 불변)

**DoD**: 동일 Job을 2회 실행해도 DB 행 수가 증가하지 않음(멱등). 앱 재시작 후에도 메타데이터가 유지되어 재시작이 정상 인식됨.

---

### Phase 4 — 확장 (Scaling)
**목표**: 대용량 가정 하에 처리량을 높이는 두 가지 전략을 실습·비교

> **[Executor 선택 — S-5 반영]** `SimpleAsyncTaskExecutor`는 스레드 수 제한·풀 재사용이 없어 대용량에서 OOM 위험이 있다. 학습 단계에서도 `ThreadPoolTaskExecutor`를 사용하고 `corePoolSize`, `maxPoolSize`, `queueCapacity`를 직접 설정한다.

#### 4-1. 멀티스레드 Step
> **[M-1 반영]** `FlatFileItemReader`는 상태를 가지는(stateful) 컴포넌트로, 멀티스레드 Step에서 공유하면 행 중복/누락이 발생한다. **`SynchronizedItemStreamReader`로 래핑**하거나 파티셔닝(4-2)을 우선 적용한다.

```java
return new StepBuilder("transactionStep", jobRepository)
    .<TransactionInput, Transaction>chunk(10, transactionManager)
    .reader(synchronizedReader)   // SynchronizedItemStreamReader 래핑
    .writer(writer)
    .taskExecutor(threadPoolTaskExecutor)
    .build();
```
- [ ] `ThreadPoolTaskExecutor` 빈 등록 및 적용
- [ ] `FlatFileItemReader`를 `SynchronizedItemStreamReader`로 래핑

#### 4-2. 파티셔닝 (Partitioning)
- [ ] `Partitioner` 구현
- [ ] **[m-2 반영]** worker Step의 `ItemReader`/`ItemWriter`를 `@StepScope`로 선언(Step 빈이 아님). 파티셔닝에서 각 파티션이 독립 Reader 인스턴스를 갖도록 보장하고 `stepExecutionContext` 값을 SpEL로 주입받는다.
- [ ] **[S-3 반영]** 파티셔닝 기준 결정: 데이터가 11-01~11-05(5일)에 분포하므로 **일자 기준**이 자연스럽다. 단 `gridSize`를 데이터 분포에 맞춰(예: 5) 설정해 빈 파티션이 생기지 않게 한다. id 범위 기준은 데이터가 균등 분포일 때 선택.

```java
@Bean
public Step managerStep(JobRepository jobRepository, Partitioner partitioner, Step workerStep) {
    return new StepBuilder("managerStep", jobRepository)
        .partitioner("workerStep", partitioner)
        .step(workerStep)
        .gridSize(5)   // 일자 수에 맞춤 (빈 파티션 방지)
        .taskExecutor(threadPoolTaskExecutor)
        .build();
}
```

**DoD**: 단일스레드 대비 처리 시간/로그를 비교하고 차이를 문서화. 멀티스레드/파티셔닝 환경에서 행 중복·누락이 없음을 검증.

---

### Phase 5 — 집계 Step 추가
**목표**: 적재된 거래를 사용자/일자별로 집계하는 후속 Step을 Job에 연결

- [ ] `UserDailySummary` 엔티티 구현(3.3절 설계)
- [ ] `JpaPagingItemReader` 또는 쿼리 기반 Reader로 거래 조회
- [ ] 사용자·일자별 합계 집계 Processor/Writer 구성
- [ ] `Job` 에 `step1(적재) → step2(집계)` 순차 흐름 구성
- [ ] (선택) `spring.batch.job.enabled=false` + `JobLauncher.run()` 직접 호출 방식 실습

**DoD**: 집계 결과 테이블(`UserDailySummary`)에 사용자/일자별 거래 합계·건수가 생성되고, 재실행 시 복합 유니크 키로 중복이 발생하지 않음.

---

## 5. 테스트 계획

| 레벨 | 대상 | 도구 | 비고 |
|------|------|------|------|
| 단위 | Processor 검증/변환 로직 | **순수 JUnit 5 (Spring 컨텍스트 없음)** | m-3 반영 — 빠른 피드백 |
| 통합 | Step/Job 단위 실행 | `spring-batch-test` (`JobLauncherTestUtils`, `JobRepositoryTestUtils`) | 컨텍스트 로딩 비용 큼 |
| 시나리오 | Skip/Retry, 재시작, 멱등성 | 오류 주입 CSV + 통합 테스트 | skipLimit 초과 FAILED 포함 |

```java
// 단위 테스트: Processor는 컨텍스트 없이 순수 JUnit
class TransactionProcessorTest {
    @Test
    void rejects_negative_amount() { /* ... */ }
}

// 통합 테스트 골격
@SpringBatchTest
@SpringBootTest
class DailyTransactionJobTest {
    @Autowired JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void job_completes_and_persists_valid_rows() throws Exception {
        JobExecution execution = jobLauncherTestUtils.launchJob();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }
}
```

---

## 6. 리스크 및 대응

| 리스크 | 영향 | 대응 |
|--------|------|------|
| 멀티스레드 Reader 상태 공유 | 데이터 누락/중복 | `SynchronizedItemStreamReader` 래핑 또는 파티셔닝 우선 (M-1) |
| `ddl-auto=create-drop` 유지 | Phase 3 재시작 메타데이터 유실 | Phase 3 이전 파일 모드 H2 + `ddl-auto=update`로 전환 (C-3) |
| H2 인메모리 휘발성 | 재시작 검증 어려움 | 파일 모드 H2로 메타데이터 영속화 |
| 멱등성 미보장 | 재실행 시 중복 적재 | id PK + `JpaItemWriter.merge()` 동작 검증 (C-2) |
| JobParameters 미설계 | 반복 실행 불가(예외) | 유니크 파라미터(`run.id`/`date`) 전달 (M-4) |
| 청크 크기 부적절 | 학습 효과 저하 / 메모리·성능 | 데이터보다 작게(5~10) 설정 후 분포에 맞춰 튜닝 (M-5) |
| `SimpleAsyncTaskExecutor` 사용 | 무제한 스레드 → OOM | `ThreadPoolTaskExecutor` + 풀 크기 설정 (S-5) |

---

## 7. 산출물

- [ ] 배치 소스 코드 (`domain`, `job`, `reader`, `processor`, `writer`)
- [ ] 단위/통합 테스트
- [ ] 단계별 학습 기록(README 로드맵 갱신)
- [ ] 확장 전략(멀티스레드 vs 파티셔닝) 비교 노트

---

## 8. 참고 자료

- Spring Batch Reference — Chunk-Oriented Processing / Fault Tolerance / Scalability (v5.2.x, Context7 조회)
- [Spring Batch 공식 레퍼런스](https://docs.spring.io/spring-batch/reference/)
- [Spring Boot Batch 가이드](https://docs.spring.io/spring-boot/reference/howto/batch.html)
