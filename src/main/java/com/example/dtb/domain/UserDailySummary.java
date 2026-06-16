package com.example.dtb.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * 사용자/일자별 거래 집계 결과.
 *
 * (userId, txDate) 복합 PK: JpaItemWriter.merge()가 동일 키 존재 시 UPDATE,
 * 없으면 INSERT → 잡 재실행 시 중복 없이 최신 집계값으로 갱신된다.
 */
@Entity
@Table(name = "dtb_user_daily_summary")
@IdClass(UserDailySummaryId.class)
@Getter
@NoArgsConstructor
public class UserDailySummary {

    @Id
    @Column(nullable = false)
    private Long userId;

    @Id
    @Column(nullable = false)
    private LocalDate txDate;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal totalAmount;

    @Column(nullable = false)
    private int txCount;

    public UserDailySummary(Long userId, LocalDate txDate, BigDecimal totalAmount, int txCount) {
        this.userId = userId;
        this.txDate = txDate;
        this.totalAmount = totalAmount;
        this.txCount = txCount;
    }
}
