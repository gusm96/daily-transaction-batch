package com.example.dtb.batch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 내결함성 검증 — 음수 금액 3건(skipLimit 이내): 정상 행만 저장되고 Job은 COMPLETED
 *
 * transactions-with-errors.csv: 8행 중 id=2,4,7 이 음수 금액 → 3 skips (skipLimit=3 허용)
 */
@SpringBatchTest
@SpringBootTest
@TestPropertySource(properties = "batch.input.file=classpath:transactions-with-errors.csv")
class FaultToleranceIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private Job dailyTransactionJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(dailyTransactionJob);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    void job_completes_and_persists_only_valid_rows_when_within_skip_limit() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // 유효 행 5건만 저장됨 (id=1,3,5,6,8)
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM dtb_transaction", Integer.class);
        assertThat(count).isEqualTo(5);

        // Step의 skipCount == 3 (음수 행 id=2,4,7 각각 skip)
        StepExecution stepExecution = execution.getStepExecutions().iterator().next();
        assertThat(stepExecution.getSkipCount()).isEqualTo(3);
    }
}
