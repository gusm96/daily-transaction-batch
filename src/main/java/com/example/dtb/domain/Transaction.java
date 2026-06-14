package com.example.dtb.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(name = "dtb_transaction")
@Getter
@NoArgsConstructor
public class Transaction {

    @Id
    private Long id;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false)
    private LocalDate createdAt;

    public Transaction(Long id, Long userId, BigDecimal amount, LocalDate createdAt) {
        this.id = id;
        this.userId = userId;
        this.amount = amount;
        this.createdAt = createdAt;
    }
}
