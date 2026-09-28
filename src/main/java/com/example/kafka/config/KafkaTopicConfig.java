package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String STRING_TOPIC = "String-Message";
    public static final String JSON_TOPIC = "JSON-Message";

    @Bean
    public NewTopic stringTopic() {
        return TopicBuilder.name(STRING_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic jsonTopic() {
        return TopicBuilder.name(JSON_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
