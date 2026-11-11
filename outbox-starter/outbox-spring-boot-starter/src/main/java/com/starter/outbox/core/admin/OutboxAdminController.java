package com.starter.outbox.core.admin;

import com.starter.outbox.core.dispatcher.OutboxDispatcher;
import com.starter.outbox.core.entity.OutboxEvent;
import com.starter.outbox.core.entity.OutboxStatus;
import com.starter.outbox.core.repository.OutboxEventRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Admin REST endpoints for inspecting and managing outbox events.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code GET  /admin/outbox/stats} — counts by status</li>
 *   <li>{@code GET  /admin/outbox/events?status=...&limit=...} — list events</li>
 *   <li>{@code GET  /admin/outbox/events/{id}} — single event detail</li>
 *   <li>{@code POST /admin/outbox/events/{id}/retry} — reset for reprocessing</li>
 *   <li>{@code POST /admin/outbox/events/retry-all-dead-letter} — bulk retry DLQ</li>
 *   <li>{@code POST /admin/outbox/dispatcher/pause} — pause the dispatcher</li>
 *   <li>{@code POST /admin/outbox/dispatcher/resume} — resume the dispatcher</li>
 * </ul>
 */
@RestController
@RequestMapping("${outbox.admin.base-path:/admin/outbox}")
public class OutboxAdminController {

    private final OutboxEventRepository repository;
    private final OutboxDispatcher dispatcher;

    public OutboxAdminController(OutboxEventRepository repository,
                                 OutboxDispatcher dispatcher) {
        this.repository = repository;
        this.dispatcher = dispatcher;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (OutboxStatus status : OutboxStatus.values()) {
            result.put(status.name(), repository.countByStatus(status));
        }
        result.put("dispatcherEnabled", dispatcher.isEnabled());
        return result;
    }

    @GetMapping("/events")
    public List<OutboxEvent> listEvents(
            @RequestParam(defaultValue = "DEAD_LETTER") OutboxStatus status,
            @RequestParam(defaultValue = "20") int limit) {
        return repository.findByStatusOrderByCreatedAtDesc(status, PageRequest.of(0, limit));
    }

    @GetMapping("/events/{id}")
    public ResponseEntity<OutboxEvent> getEvent(@PathVariable String id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/events/{id}/retry")
    @Transactional
    public ResponseEntity<OutboxEvent> retryEvent(@PathVariable String id) {
        return repository.findById(id)
                .map(event -> {
                    event.resetForRetry();
                    return ResponseEntity.ok(repository.save(event));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/events/retry-all-dead-letter")
    @Transactional
    public Map<String, Object> retryAllDeadLetter(
            @RequestParam(required = false) String destination) {
        List<OutboxEvent> deadLettered;
        if (destination != null) {
            deadLettered = repository.findByStatusAndDestinationOrderByCreatedAtDesc(
                    OutboxStatus.DEAD_LETTER, destination, PageRequest.of(0, 1000));
        } else {
            deadLettered = repository.findByStatusOrderByCreatedAtDesc(
                    OutboxStatus.DEAD_LETTER, PageRequest.of(0, 1000));
        }
        deadLettered.forEach(OutboxEvent::resetForRetry);
        repository.saveAll(deadLettered);
        return Map.of("retriedCount", deadLettered.size());
    }

    @PostMapping("/dispatcher/pause")
    public Map<String, Object> pauseDispatcher() {
        dispatcher.pause();
        return Map.of("status", "paused");
    }

    @PostMapping("/dispatcher/resume")
    public Map<String, Object> resumeDispatcher() {
        dispatcher.resume();
        return Map.of("status", "resumed");
    }
}
