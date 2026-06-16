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

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5 — 집계 Step 검증
 *
 * dailyFullBatchJob: CSV 25건 로딩 → (user_id, tx_date) 별 집계
 *
 * 검증:
 *   - dtb_transaction: 25건
 *   - dtb_user_daily_summary: 25건 (각 (user_id, date) 조합이 유일)
 *   - user_id=101, tx_date=2025-11-01: total_amount=5000, tx_count=1
 *   - 재실행 멱등성: 2회 실행 후에도 summary 행 수 불변
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
class AggregationIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private Job dailyFullBatchJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(dailyFullBatchJob);
        jobRepositoryTestUtils.removeJobExecutions();
        jdbcTemplate.execute("DELETE FROM dtb_user_daily_summary");
        jdbcTemplate.execute("DELETE FROM dtb_transaction");
    }

    @Test
    void full_batch_job_loads_transactions_and_aggregates_by_user_and_date() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Integer txCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dtb_transaction", Integer.class);
        assertThat(txCount).isEqualTo(25);

        // (user_id, tx_date) 조합별 집계 결과
        Integer summaryCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dtb_user_daily_summary", Integer.class);
        assertThat(summaryCount).isEqualTo(25);

        // user_id=101, tx_date=2025-11-01: id=1, amount=5000 (1건)
        BigDecimal total = jdbcTemplate.queryForObject(
                "SELECT total_amount FROM dtb_user_daily_summary WHERE user_id = 101 AND tx_date = '2025-11-01'",
                BigDecimal.class);
        assertThat(total).isEqualByComparingTo(new BigDecimal("5000"));

        Integer count = jdbcTemplate.queryForObject(
                "SELECT tx_count FROM dtb_user_daily_summary WHERE user_id = 101 AND tx_date = '2025-11-01'",
                Integer.class);
        assertThat(count).isEqualTo(1);
    }

    @Test
    void aggregation_is_idempotent_on_repeated_runs() throws Exception {
        JobParameters params1 = new JobParametersBuilder()
                .addLong("run.id", 1L)
                .toJobParameters();
        JobExecution exec1 = jobLauncherTestUtils.launchJob(params1);
        assertThat(exec1.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        JobParameters params2 = new JobParametersBuilder()
                .addLong("run.id", 2L)
                .toJobParameters();
        JobExecution exec2 = jobLauncherTestUtils.launchJob(params2);
        assertThat(exec2.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // 2회 실행 후에도 summary 행 수 불변 (merge → UPDATE)
        Integer summaryCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dtb_user_daily_summary", Integer.class);
        assertThat(summaryCount).isEqualTo(25);
    }
}
