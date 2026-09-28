package com.example.kafka.producer;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AtomicLong producedCount = new AtomicLong(0);

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, Object>> sendMessage(String message, String key) {
        String messageKey = (key != null && !key.isBlank()) ? key : "key-" + System.currentTimeMillis();
        log.info("Sending message to topic '{}': key='{}', payload='{}'", KafkaTopicConfig.STRING_TOPIC, messageKey, message);
        
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopicConfig.STRING_TOPIC, messageKey, message);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                producedCount.incrementAndGet();
                log.info("Sent message='{}' to partition={} with offset={}",
                        message,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message='{}' due to: {}", message, ex.getMessage(), ex);
            }
        });

        return future;
    }

    public CompletableFuture<SendResult<String, Object>> sendOrderEvent(OrderEvent orderEvent) {
        String key = orderEvent.orderId();
        log.info("Sending OrderEvent to topic '{}': key='{}', orderId='{}'", KafkaTopicConfig.JSON_TOPIC, key, orderEvent.orderId());
        
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopicConfig.JSON_TOPIC, key, orderEvent);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                producedCount.incrementAndGet();
                log.info("Sent OrderEvent='{}' to partition={} with offset={}",
                        orderEvent.orderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send OrderEvent='{}' due to: {}", orderEvent.orderId(), ex.getMessage(), ex);
            }
        });

        return future;
    }

    public long getProducedCount() {
        return producedCount.get();
    }
}
