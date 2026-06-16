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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 — Part 2 검증: 파티셔닝
 *
 * 25행 CSV를 5개 파티션으로 분할 처리.
 * - 전체 데이터 정합성 (25건 저장)
 * - 5개 워커 Step 실행 확인 (stepName 패턴: partitionWorkerStep:partitionN)
 */
@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
class PartitioningIntegrationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    private Job partitionedTransactionJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(partitionedTransactionJob);
        jobRepositoryTestUtils.removeJobExecutions();
        jdbcTemplate.execute("DELETE FROM dtb_transaction");
    }

    @Test
    void partitioned_job_saves_all_rows_across_five_partitions() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        JobExecution execution = jobLauncherTestUtils.launchJob(params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM dtb_transaction", Integer.class);
        assertThat(count).isEqualTo(25);

        // 5개 워커 Step 실행 확인
        List<String> workerStepNames = execution.getStepExecutions().stream()
                .map(StepExecution::getStepName)
                .filter(name -> name.startsWith("partitionWorkerStep:"))
                .toList();
        assertThat(workerStepNames).hasSize(5);
    }
}
