package com.example.dtb.batch;

import com.example.dtb.batch.exception.InvalidTransactionException;
import com.example.dtb.batch.listener.TransactionSkipListener;
import com.example.dtb.batch.processor.TransactionItemProcessor;
import com.example.dtb.domain.Transaction;
import com.example.dtb.domain.TransactionInput;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;

/**
 * Phase 4 — Part 1: 멀티스레드 Step
 *
 * FlatFileItemReader는 내부 상태(현재 라인 포지션)를 가지므로 thread-safe하지 않다.
 * SynchronizedItemStreamReader로 감싸 read() 호출을 직렬화하면 여러 스레드가 안전하게 공유할 수 있다.
 */
@Configuration
public class MultiThreadedJobConfig {

    private static final int THREAD_COUNT = 4;

    @Bean
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(THREAD_COUNT);
        executor.setMaxPoolSize(THREAD_COUNT);
        executor.setThreadNamePrefix("batch-thread-");
        executor.initialize();
        return executor;
    }

    @Bean
    public SynchronizedItemStreamReader<TransactionInput> synchronizedTransactionReader(
            @Value("${batch.input.file:classpath:transactions.csv}") Resource resource) {
        FlatFileItemReader<TransactionInput> delegate = new FlatFileItemReaderBuilder<TransactionInput>()
                .name("synchronizedTransactionReader")
                .resource(resource)
                .linesToSkip(1)
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

        return new SynchronizedItemStreamReaderBuilder<TransactionInput>()
                .delegate(delegate)
                .build();
    }

    @Bean
    public Step multiThreadedTransactionStep(JobRepository jobRepository,
                                             PlatformTransactionManager transactionManager,
                                             SynchronizedItemStreamReader<TransactionInput> synchronizedTransactionReader,
                                             TransactionItemProcessor transactionItemProcessor,
                                             JpaItemWriter<Transaction> transactionItemWriter,
                                             TransactionSkipListener transactionSkipListener,
                                             TaskExecutor batchTaskExecutor) {
        return new StepBuilder("multiThreadedTransactionStep", jobRepository)
                .<TransactionInput, Transaction>chunk(5, transactionManager)
                .reader(synchronizedTransactionReader)
                .processor(transactionItemProcessor)
                .writer(transactionItemWriter)
                .faultTolerant()
                .skip(InvalidTransactionException.class)
                .skip(FlatFileParseException.class)
                .skipLimit(3)
                .retry(CannotAcquireLockException.class)
                .retryLimit(3)
                .listener(transactionSkipListener)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Job multiThreadedTransactionJob(JobRepository jobRepository, Step multiThreadedTransactionStep) {
        return new JobBuilder("multiThreadedTransactionJob", jobRepository)
                .start(multiThreadedTransactionStep)
                .build();
    }
}
