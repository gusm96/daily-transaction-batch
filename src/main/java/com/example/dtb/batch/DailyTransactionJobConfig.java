package com.example.dtb.batch;

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
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;

@Configuration
public class DailyTransactionJobConfig {

    // -- Reader --

    @Bean
    public FlatFileItemReader<TransactionInput> transactionItemReader() {
        return new FlatFileItemReaderBuilder<TransactionInput>()
                .name("transactionItemReader")
                .resource(new ClassPathResource("transactions.csv"))
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
                                JpaItemWriter<Transaction> transactionItemWriter) {
        return new StepBuilder("transactionStep", jobRepository)
                .<TransactionInput, Transaction>chunk(10, transactionManager)  // M-5: 데이터(25건)보다 작게
                .reader(transactionItemReader)
                .processor(transactionItemProcessor)
                .writer(transactionItemWriter)
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
