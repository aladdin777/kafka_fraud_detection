package com.fraud.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransactionMetrics {
    private long count = 0;
    private double totalAmount = 0.0;

    public TransactionMetrics increment(double amount) {
        this.count++;
        this.totalAmount += amount;
        return this;
    }
}

