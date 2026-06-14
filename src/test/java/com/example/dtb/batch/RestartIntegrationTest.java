package com.example.dtb.batch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 재시작 메커니즘 검증
 *
 * Spring Batch 재시작 규칙:
 * - COMPLETED JobInstance: 같은 파라미터로 재실행 불가 (JobInstanceAlreadyCompleteException)
 * - FAILED JobInstance: 같은 파라미터로 재실행 → 새 JobExecution을 이전 실패 컨텍스트(ExecutionContext)에서 재개
 *
 * 검증 방법: date 파라미터로 고정된 JobInstance를 생성해 두 실행이 같은 JobInstance에 속하는지 확인
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input.file=classpath:transactions-exceed-skiplimit.csv")
class RestartIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private Job dailyTransactionJob;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(dailyTransactionJob);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    void restart_with_same_params_creates_new_execution_for_same_job_instance() throws Exception {
        // date를 식별 파라미터로 사용 — run.id를 쓰면 매번 새 JobInstance가 생성되어 재시작 불가
        JobParameters params = new JobParametersBuilder()
                .addString("date", "2025-11-01")
                .toJobParameters();

        // 1차 실행: skipLimit(3) 초과 → FAILED
        JobExecution exec1 = jobLauncherTestUtils.launchJob(params);
        assertThat(exec1.getStatus()).isEqualTo(BatchStatus.FAILED);

        // 동일 파라미터로 재시작 — JobLauncher가 FAILED 실행을 감지해 재시작 처리
        // (COMPLETED였다면 JobInstanceAlreadyCompleteException 발생)
        JobExecution exec2 = jobLauncherTestUtils.launchJob(params);
        assertThat(exec2.getStatus()).isEqualTo(BatchStatus.FAILED); // CSV 동일하므로 재실패

        // 핵심 검증: 두 실행이 동일한 JobInstance에 속함 → 재시작임을 증명
        assertThat(exec2.getJobInstance().getInstanceId())
                .isEqualTo(exec1.getJobInstance().getInstanceId());

        // 두 실행은 서로 다른 JobExecution (1차 실패 / 2차 재시작)
        assertThat(exec2.getId()).isNotEqualTo(exec1.getId());
    }
}
