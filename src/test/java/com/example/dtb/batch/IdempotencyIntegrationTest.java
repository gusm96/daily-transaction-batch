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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 멱등성 검증 — C-2 연계
 *
 * JpaItemWriter.merge() 동작: 동일 id가 존재하면 UPDATE, 없으면 INSERT
 * → 동일 CSV로 Job을 2회 실행해도 dtb_transaction 행 수가 증가하지 않음
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
class IdempotencyIntegrationTest {

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
    void row_count_unchanged_after_two_runs_with_same_csv() throws Exception {
        // 1차 실행
        JobParameters params1 = new JobParametersBuilder()
                .addLong("run.id", 1L)
                .toJobParameters();
        JobExecution exec1 = jobLauncherTestUtils.launchJob(params1);
        assertThat(exec1.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Integer countAfterFirst = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dtb_transaction", Integer.class);
        assertThat(countAfterFirst).isEqualTo(25);

        // 2차 실행 (다른 run.id → 새 JobInstance, 동일 CSV 처리)
        // JpaItemWriter.merge(): 기존 id → UPDATE, 없는 id → INSERT
        JobParameters params2 = new JobParametersBuilder()
                .addLong("run.id", 2L)
                .toJobParameters();
        JobExecution exec2 = jobLauncherTestUtils.launchJob(params2);
        assertThat(exec2.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Integer countAfterSecond = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dtb_transaction", Integer.class);
        // merge()로 UPDATE만 발생 → 행 수 불변 (중복 없음)
        assertThat(countAfterSecond).isEqualTo(25);
    }
}
