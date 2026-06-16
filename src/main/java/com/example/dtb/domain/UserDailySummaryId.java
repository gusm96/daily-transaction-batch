package com.example.dtb.domain;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

public class UserDailySummaryId implements Serializable {

    private Long userId;
    private LocalDate txDate;

    public UserDailySummaryId() {}

    public UserDailySummaryId(Long userId, LocalDate txDate) {
        this.userId = userId;
        this.txDate = txDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserDailySummaryId that)) return false;
        return Objects.equals(userId, that.userId) && Objects.equals(txDate, that.txDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, txDate);
    }
}
