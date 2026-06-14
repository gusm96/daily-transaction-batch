package com.example.dtb.batch.runner;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BatchJobRunner implements ApplicationRunner {

    private final Job dailyTransactionJob;
    private final JobLauncher jobLauncher;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // M-4: 매 실행마다 고유한 run.id 전달 → JobInstanceAlreadyCompleteException 방지
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();

        log.info("Starting dailyTransactionJob — params: {}", params);
        JobExecution execution = jobLauncher.run(dailyTransactionJob, params);
        log.info("Job finished — status: {}", execution.getStatus());
    }
}
