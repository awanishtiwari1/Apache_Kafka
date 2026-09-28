package com.example.kafka.dto;

import java.time.LocalDateTime;

public record OrderEvent(
        String orderId,
        String customerName,
        double amount,
        String status,
        String timestamp
) {
    public OrderEvent {
        if (timestamp == null || timestamp.isBlank()) {
            timestamp = LocalDateTime.now().toString();
        }
    }
}
