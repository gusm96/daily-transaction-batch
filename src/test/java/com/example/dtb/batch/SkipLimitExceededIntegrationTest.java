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
 * Phase 2 내결함성 검증 — 음수 금액 4건(skipLimit 초과): Job이 FAILED로 전환됨
 *
 * transactions-exceed-skiplimit.csv: 6행 중 id=1,2,3,4 가 음수 금액 → 4번째 skip이 skipLimit(3) 초과
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "batch.input.file=classpath:transactions-exceed-skiplimit.csv")
class SkipLimitExceededIntegrationTest {

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
    void job_fails_when_skip_limit_exceeded() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(params);

        // skipLimit(3) 초과 시 Job이 FAILED로 전환됨
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
    }
}
