package com.example.dtb.batch.processor;

import com.example.dtb.batch.exception.InvalidTransactionException;
import com.example.dtb.domain.Transaction;
import com.example.dtb.domain.TransactionInput;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionItemProcessorTest {

    private final TransactionItemProcessor processor = new TransactionItemProcessor();

    @Test
    void converts_valid_input_to_transaction() throws Exception {
        TransactionInput input = buildInput(1L, 101L, "5000", LocalDate.of(2025, 11, 1));

        Transaction result = processor.process(input);

        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getUserId()).isEqualTo(101L);
        assertThat(result.getAmount()).isEqualByComparingTo("5000");
        assertThat(result.getCreatedAt()).isEqualTo(LocalDate.of(2025, 11, 1));
    }

    @Test
    void rejects_negative_amount() {
        TransactionInput input = buildInput(99L, 101L, "-100", LocalDate.of(2025, 11, 1));

        assertThatThrownBy(() -> processor.process(input))
                .isInstanceOf(InvalidTransactionException.class)
                .hasMessageContaining("-100");
    }

    @Test
    void accepts_zero_amount() throws Exception {
        TransactionInput input = buildInput(2L, 101L, "0", LocalDate.of(2025, 11, 1));

        Transaction result = processor.process(input);

        assertThat(result.getAmount()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    private TransactionInput buildInput(Long id, Long userId, String amount, LocalDate createdAt) {
        TransactionInput input = new TransactionInput();
        input.setId(id);
        input.setUserId(userId);
        input.setAmount(new BigDecimal(amount));
        input.setCreatedAt(createdAt);
        return input;
    }
}
