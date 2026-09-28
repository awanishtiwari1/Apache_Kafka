package com.example.kafka.dto;

import java.time.Instant;
import java.util.UUID;

public class MessageDto {
    private String id;
    private String topic;
    private int partition;
    private long offset;
    private String key;
    private Object payload;
    private String messageType;
    private long timestamp;

    public MessageDto() {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now().toEpochMilli();
    }

    public MessageDto(String topic, int partition, long offset, String key, Object payload, String messageType) {
        this.id = UUID.randomUUID().toString();
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.payload = payload;
        this.messageType = messageType;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
