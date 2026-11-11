package com.example.orders.controller;

import com.example.orders.domain.Order;
import com.example.orders.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    public record CreateOrderRequest(String email, String product, BigDecimal amount) {}

    @PostMapping("/kafka")
    @ResponseStatus(HttpStatus.CREATED)
    public Order createKafka(@RequestBody CreateOrderRequest req) {
        return orderService.createOrderWithKafka(req.email(), req.product(), req.amount());
    }

    @PostMapping("/rest")
    @ResponseStatus(HttpStatus.CREATED)
    public Order createRest(@RequestBody CreateOrderRequest req) {
        return orderService.createOrderWithRestNotification(req.email(), req.product(), req.amount());
    }

    @PostMapping("/db")
    @ResponseStatus(HttpStatus.CREATED)
    public Order createDb(@RequestBody CreateOrderRequest req) {
        return orderService.createOrderWithAuditDb(req.email(), req.product(), req.amount());
    }

    @PostMapping("/fanout")
    @ResponseStatus(HttpStatus.CREATED)
    public Order createFanOut(@RequestBody CreateOrderRequest req) {
        return orderService.createOrderWithFanOut(req.email(), req.product(), req.amount());
    }
}
