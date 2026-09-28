package com.example.kafka.consumer;

import com.example.kafka.config.KafkaTopicConfig;
import com.example.kafka.dto.MessageDto;
import com.example.kafka.dto.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final int MAX_MESSAGES = 100;

    private final ConcurrentLinkedQueue<MessageDto> messageQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong consumedCount = new AtomicLong(0);

    @KafkaListener(topics = KafkaTopicConfig.STRING_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void listenStringTopic(ConsumerRecord<String, Object> record) {
        log.info("Received message from topic '{}': key='{}', partition={}, offset={}, payload='{}'",
                record.topic(), record.key(), record.partition(), record.offset(), record.value());

        MessageDto dto = new MessageDto(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                "STRING"
        );

        addMessage(dto);
    }

    @KafkaListener(topics = KafkaTopicConfig.JSON_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void listenJsonTopic(ConsumerRecord<String, Object> record) {
        log.info("Received OrderEvent from topic '{}': key='{}', partition={}, offset={}, payload='{}'",
                record.topic(), record.key(), record.partition(), record.offset(), record.value());

        MessageDto dto = new MessageDto(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                "ORDER"
        );

        addMessage(dto);
    }

    private void addMessage(MessageDto dto) {
        consumedCount.incrementAndGet();
        messageQueue.add(dto);
        while (messageQueue.size() > MAX_MESSAGES) {
            messageQueue.poll();
        }
    }

    public List<MessageDto> getMessages() {
        List<MessageDto> list = new ArrayList<>(messageQueue);
        Collections.reverse(list); // return newest first
        return list;
    }

    public void clearMessages() {
        messageQueue.clear();
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }
}
