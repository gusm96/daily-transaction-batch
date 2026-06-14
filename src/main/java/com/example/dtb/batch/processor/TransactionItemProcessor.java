package com.example.dtb.batch.processor;

import com.example.dtb.batch.exception.InvalidTransactionException;
import com.example.dtb.domain.Transaction;
import com.example.dtb.domain.TransactionInput;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
public class TransactionItemProcessor implements ItemProcessor<TransactionInput, Transaction> {

    @Override
    public Transaction process(TransactionInput input) {
        if (input.getAmount().compareTo(BigDecimal.ZERO) < 0) {
            log.warn("Invalid transaction — negative amount: id={}, amount={}", input.getId(), input.getAmount());
            throw new InvalidTransactionException(
                    "Negative amount [" + input.getAmount() + "] for transaction id=" + input.getId());
        }
        return new Transaction(input.getId(), input.getUserId(), input.getAmount(), input.getCreatedAt());
    }
}
