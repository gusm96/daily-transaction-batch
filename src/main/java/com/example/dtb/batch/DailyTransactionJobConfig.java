package com.example.dtb.batch;

import com.example.dtb.batch.exception.InvalidTransactionException;
import com.example.dtb.batch.listener.TransactionSkipListener;
import com.example.dtb.batch.processor.TransactionItemProcessor;
import com.example.dtb.domain.Transaction;
import com.example.dtb.domain.TransactionInput;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;

@Configuration
public class DailyTransactionJobConfig {

    // -- Reader --

    @Bean
    public FlatFileItemReader<TransactionInput> transactionItemReader(
            // 테스트에서 @TestPropertySource로 오버라이드 가능 (기본: transactions.csv)
            @Value("${batch.input.file:classpath:transactions.csv}") Resource resource) {
        return new FlatFileItemReaderBuilder<TransactionInput>()
                .name("transactionItemReader")
                .resource(resource)
                .linesToSkip(1)
                .delimited()
                // 컬럼 위치 순서대로 DTO 필드명 지정 (CSV: id, user_id, amount, created_at)
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

    // -- Writer --

    @Bean
    public JpaItemWriter<Transaction> transactionItemWriter(EntityManagerFactory entityManagerFactory) {
        return new JpaItemWriterBuilder<Transaction>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    // -- Step --

    @Bean
    public Step transactionStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                FlatFileItemReader<TransactionInput> transactionItemReader,
                                TransactionItemProcessor transactionItemProcessor,
                                JpaItemWriter<Transaction> transactionItemWriter,
                                TransactionSkipListener transactionSkipListener) {
        return new StepBuilder("transactionStep", jobRepository)
                .<TransactionInput, Transaction>chunk(10, transactionManager)
                .reader(transactionItemReader)
                .processor(transactionItemProcessor)
                .writer(transactionItemWriter)
                .faultTolerant()
                // S-1: 커스텀 검증 예외(InvalidTransactionException)와 CSV 파싱 오류를 skip 처리
                // (jakarta.validation.ValidationException과 혼동 주의)
                .skip(InvalidTransactionException.class)
                .skip(FlatFileParseException.class)
                // M-2: skipLimit 근거 — 학습용 소량 데이터(25건) 기준 최대 3건(약 12%) 허용
                .skipLimit(3)
                // 일시적 DB 잠금 오류는 최대 3회 재시도 (DeadlockLoserDataAccessException은 Spring 6.x deprecated)
                .retry(CannotAcquireLockException.class)
                .retryLimit(3)
                // m-4: @Component Bean 주입 (new TransactionSkipListener() 직접 생성 금지)
                .listener(transactionSkipListener)
                .build();
    }

    // -- Job --

    @Bean
    public Job dailyTransactionJob(JobRepository jobRepository, Step transactionStep) {
        return new JobBuilder("dailyTransactionJob", jobRepository)
                .start(transactionStep)
                .build();
    }
}
