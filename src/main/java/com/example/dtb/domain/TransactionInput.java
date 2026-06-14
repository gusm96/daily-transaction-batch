package com.example.dtb.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
public class TransactionInput {

    private Long id;
    private Long userId;
    private BigDecimal amount;
    private LocalDate createdAt;
}
