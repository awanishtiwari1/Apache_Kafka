package com.example.kafka.controller;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/logs")
@CrossOrigin(origins = "*")
public class LogController {

    private static final String LOG_DIR = "logs";

    /**
     * Fetch content of today's log file (e.g. logs/2026-09-28.txt)
     */
    @GetMapping("/today")
    public ResponseEntity<Map<String, Object>> getTodayLog() {
        String todayFileName = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + ".txt";
        Path logFilePath = Paths.get(LOG_DIR, todayFileName);

        Map<String, Object> response = new HashMap<>();
        response.put("filename", todayFileName);
        response.put("filePath", logFilePath.toAbsolutePath().toString());

        if (!Files.exists(logFilePath)) {
            response.put("exists", false);
            response.put("content", "Log file for today (" + todayFileName + ") has not been created yet or is empty.");
            return ResponseEntity.ok(response);
        }

        try {
            String content = Files.readString(logFilePath);
            response.put("exists", true);
            response.put("sizeBytes", Files.size(logFilePath));
            response.put("content", content);
            return ResponseEntity.ok(response);
        } catch (IOException e) {
            response.put("error", "Failed to read log file: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * List all log files stored in the logs folder
     */
    @GetMapping("/files")
    public ResponseEntity<List<Map<String, Object>>> listLogFiles() {
        File folder = new File(LOG_DIR);
        if (!folder.exists() || !folder.isDirectory()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt"));
        if (files == null) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<Map<String, Object>> result = Arrays.stream(files)
                .map(file -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", file.getName());
                    map.put("size", file.length());
                    map.put("lastModified", file.lastModified());
                    map.put("path", file.getAbsolutePath());
                    return map;
                })
                .sorted((a, b) -> Long.compare((Long) b.get("lastModified"), (Long) a.get("lastModified")))
                .collect(Collectors.toList());

        return ResponseEntity.ok(result);
    }

    /**
     * Download specific notepad log file (.txt)
     */
    @GetMapping("/download/{fileName}")
    public ResponseEntity<Resource> downloadLogFile(@PathVariable String fileName) {
        if (fileName.contains("..") || fileName.contains("/") || fileName.contains("\\")) {
            return ResponseEntity.badRequest().build();
        }

        Path filePath = Paths.get(LOG_DIR, fileName);
        if (!Files.exists(filePath)) {
            return ResponseEntity.notFound().build();
        }

        Resource resource = new FileSystemResource(filePath.toFile());
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                .contentType(MediaType.TEXT_PLAIN)
                .body(resource);
    }
}
