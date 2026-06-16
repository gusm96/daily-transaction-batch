package com.example.dtb.batch;

import com.example.dtb.domain.UserDailySummary;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;

/**
 * Phase 5 — 집계 Step
 *
 * dailyFullBatchJob: transactionStep(CSV 로딩) → aggregationStep(사용자/일자별 집계)
 *
 * 집계 전략: SQL GROUP BY를 Reader 레벨에서 수행 → Processor 불필요.
 * DB가 집계를 처리하고 Spring Batch는 결과를 summary 테이블로 옮긴다.
 *
 * 멱등성: UserDailySummary의 (userId, txDate) 복합 PK로 JpaItemWriter.merge()가
 * 재실행 시 INSERT 대신 UPDATE를 수행한다.
 */
@Configuration
public class AggregationJobConfig {

    private static final String AGGREGATION_SQL = """
            SELECT user_id,
                   created_at,
                   SUM(amount)  AS total_amount,
                   COUNT(*)     AS tx_count
            FROM dtb_transaction
            GROUP BY user_id, created_at
            ORDER BY user_id, created_at
            """;

    @Bean
    public JdbcCursorItemReader<UserDailySummary> aggregationItemReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<UserDailySummary>()
                .name("aggregationItemReader")
                .dataSource(dataSource)
                .sql(AGGREGATION_SQL)
                .rowMapper((rs, rowNum) -> new UserDailySummary(
                        rs.getLong("user_id"),
                        rs.getDate("created_at").toLocalDate(),
                        rs.getBigDecimal("total_amount"),
                        rs.getInt("tx_count")
                ))
                .build();
    }

    @Bean
    public JpaItemWriter<UserDailySummary> aggregationItemWriter(EntityManagerFactory entityManagerFactory) {
        return new JpaItemWriterBuilder<UserDailySummary>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public Step aggregationStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                JdbcCursorItemReader<UserDailySummary> aggregationItemReader,
                                JpaItemWriter<UserDailySummary> aggregationItemWriter) {
        return new StepBuilder("aggregationStep", jobRepository)
                .<UserDailySummary, UserDailySummary>chunk(10, transactionManager)
                .reader(aggregationItemReader)
                .writer(aggregationItemWriter)
                .build();
    }

    /**
     * 2-Step Job: CSV 로딩 후 집계 수행.
     * transactionStep은 DailyTransactionJobConfig에서 정의된 빈을 재사용한다.
     */
    @Bean
    public Job dailyFullBatchJob(JobRepository jobRepository,
                                  Step transactionStep,
                                  Step aggregationStep) {
        return new JobBuilder("dailyFullBatchJob", jobRepository)
                .start(transactionStep)
                .next(aggregationStep)
                .build();
    }
}
