package com.example.dtb.batch;

import com.example.dtb.batch.partitioner.TransactionLineRangePartitioner;
import com.example.dtb.batch.processor.TransactionItemProcessor;
import com.example.dtb.domain.Transaction;
import com.example.dtb.domain.TransactionInput;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;

/**
 * Phase 4 — Part 2: 파티셔닝
 *
 * 25행 CSV를 5개의 라인 범위 파티션으로 분할해 병렬 처리한다.
 * 파티션마다 @StepScope Reader 인스턴스가 독립적으로 생성되므로
 * 멀티스레드 Step과 달리 Reader 동기화가 필요 없다.
 *
 * 실행 구조:
 *   partitionedTransactionStep (master)
 *     └─ partitionWorkerStep:partition0  ── 1~5행
 *     └─ partitionWorkerStep:partition1  ── 6~10행
 *     └─ partitionWorkerStep:partition2  ── 11~15행
 *     └─ partitionWorkerStep:partition3  ── 16~20행
 *     └─ partitionWorkerStep:partition4  ── 21~25행
 */
@Configuration
public class PartitionedJobConfig {

    private static final int PARTITION_COUNT = 5;
    private static final int TOTAL_DATA_LINES = 25;

    @Bean
    public TransactionLineRangePartitioner transactionLineRangePartitioner() {
        return new TransactionLineRangePartitioner(TOTAL_DATA_LINES, 1);
    }

    /**
     * @StepScope: 파티션 실행 시점에 stepExecutionContext 값을 주입받아
     * 각 파티션 전용 Reader 인스턴스를 생성한다.
     */
    @Bean
    @StepScope
    public FlatFileItemReader<TransactionInput> partitionedTransactionReader(
            @Value("${batch.input.file:classpath:transactions.csv}") Resource resource,
            @Value("#{stepExecutionContext['linesToSkip']}") Integer linesToSkip,
            @Value("#{stepExecutionContext['lineCount']}") Integer lineCount) {
        return new FlatFileItemReaderBuilder<TransactionInput>()
                .name("partitionedTransactionReader")
                .resource(resource)
                .linesToSkip(linesToSkip)
                .maxItemCount(lineCount)
                .delimited()
                .names("id", "userId", "amount", "createdAt")
                .fieldSetMapper(fieldSet -> {
                    TransactionInput input = new TransactionInput();
                    input.setId(fieldSet.readLong("id"));
                    input.setUserId(fieldSet.readLong("userId"));
                    input.setAmount(fieldSet.readBigDecimal("amount"));
                    input.setCreatedAt(LocalDate.parse(fieldSet.readString("createdAt")));
                    return input;
                })
                .build();
    }

    @Bean
    public Step partitionWorkerStep(JobRepository jobRepository,
                                    PlatformTransactionManager transactionManager,
                                    FlatFileItemReader<TransactionInput> partitionedTransactionReader,
                                    TransactionItemProcessor transactionItemProcessor,
                                    JpaItemWriter<Transaction> transactionItemWriter) {
        return new StepBuilder("partitionWorkerStep", jobRepository)
                .<TransactionInput, Transaction>chunk(5, transactionManager)
                .reader(partitionedTransactionReader)
                .processor(transactionItemProcessor)
                .writer(transactionItemWriter)
                .build();
    }

    @Bean
    public TaskExecutorPartitionHandler transactionPartitionHandler(Step partitionWorkerStep,
                                                                    TaskExecutor batchTaskExecutor) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(partitionWorkerStep);
        handler.setTaskExecutor(batchTaskExecutor);
        handler.setGridSize(PARTITION_COUNT);
        return handler;
    }

    @Bean
    public Step partitionedTransactionStep(JobRepository jobRepository,
                                           TransactionLineRangePartitioner transactionLineRangePartitioner,
                                           TaskExecutorPartitionHandler transactionPartitionHandler) {
        return new StepBuilder("partitionedTransactionStep", jobRepository)
                .partitioner("partitionWorkerStep", transactionLineRangePartitioner)
                .partitionHandler(transactionPartitionHandler)
                .build();
    }

    @Bean
    public Job partitionedTransactionJob(JobRepository jobRepository, Step partitionedTransactionStep) {
        return new JobBuilder("partitionedTransactionJob", jobRepository)
                .start(partitionedTransactionStep)
                .build();
    }
}
