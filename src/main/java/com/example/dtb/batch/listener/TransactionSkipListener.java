package com.example.dtb.batch.listener;

import com.example.dtb.domain.Transaction;
import com.example.dtb.domain.TransactionInput;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

// m-4: @Component로 Bean 등록 — 직접 인스턴스 생성 시 @Autowired 필드가 null이 됨
@Slf4j
@Component
public class TransactionSkipListener implements SkipListener<TransactionInput, Transaction> {

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("[SKIP-READ] {}: {}", t.getClass().getSimpleName(), t.getMessage());
    }

    @Override
    public void onSkipInProcess(TransactionInput item, Throwable t) {
        log.warn("[SKIP-PROCESS] id={}, userId={}, amount={} — {}: {}",
                item.getId(), item.getUserId(), item.getAmount(),
                t.getClass().getSimpleName(), t.getMessage());
    }

    @Override
    public void onSkipInWrite(Transaction item, Throwable t) {
        log.warn("[SKIP-WRITE] id={} — {}: {}", item.getId(), t.getClass().getSimpleName(), t.getMessage());
    }
}
