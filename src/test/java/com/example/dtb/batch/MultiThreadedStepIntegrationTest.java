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
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 — Part 1 검증: 멀티스레드 Step
 *
 * SynchronizedItemStreamReader로 감싼 FlatFileItemReader를 4개 스레드로 처리.
 * 데이터 정합성(25건 전부 저장), 중복 없음을 확인한다.
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
class MultiThreadedStepIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private Job multiThreadedTransactionJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(multiThreadedTransactionJob);
        jobRepositoryTestUtils.removeJobExecutions();
        jdbcTemplate.execute("DELETE FROM dtb_transaction");
    }

    @Test
    void multithreaded_job_saves_all_rows_without_duplication() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM dtb_transaction", Integer.class);
        assertThat(count).isEqualTo(25);

        StepExecution stepExecution = execution.getStepExecutions().iterator().next();
        assertThat(stepExecution.getReadCount()).isEqualTo(25);
        assertThat(stepExecution.getWriteCount()).isEqualTo(25);
    }
}
