package com.example.kafka.controller;

import com.example.kafka.consumer.KafkaConsumerService;
import com.example.kafka.dto.MessageDto;
import com.example.kafka.dto.OrderEvent;
import com.example.kafka.producer.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
@CrossOrigin(origins = "*")
public class KafkaController {

    private final KafkaProducerService producerService;
    private final KafkaConsumerService consumerService;

    public KafkaController(KafkaProducerService producerService, KafkaConsumerService consumerService) {
        this.producerService = producerService;
        this.consumerService = consumerService;
    }

    @PostMapping("/publish")
    public ResponseEntity<Map<String, Object>> publishMessage(
            @RequestParam("message") String message,
            @RequestParam(value = "key", required = false) String key) {
        
        producerService.sendMessage(message, key);
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Message queued for publishing");
        response.put("payload", message);
        response.put("key", key);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/order")
    public ResponseEntity<Map<String, Object>> publishOrder(@RequestBody OrderEvent orderEvent) {
        producerService.sendOrderEvent(orderEvent);
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Order event queued for publishing");
        response.put("order", orderEvent);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/messages")
    public ResponseEntity<List<MessageDto>> getMessages() {
        return ResponseEntity.ok(consumerService.getMessages());
    }

    @DeleteMapping("/messages")
    public ResponseEntity<Map<String, String>> clearMessages() {
        consumerService.clearMessages();
        Map<String, String> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "Message store cleared");
        return ResponseEntity.ok(response);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("status", "UP");
        stats.put("producedCount", producerService.getProducedCount());
        stats.put("consumedCount", consumerService.getConsumedCount());
        stats.put("activeTopics", List.of("String-Message", "JSON-Message"));
        return ResponseEntity.ok(stats);
    }
}
