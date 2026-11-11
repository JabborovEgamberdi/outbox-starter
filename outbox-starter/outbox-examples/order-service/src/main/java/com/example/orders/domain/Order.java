package com.example.orders.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false)
    private String customerEmail;

    @Column(nullable = false)
    private String productName;

    @Column(nullable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status = OrderStatus.CREATED;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    void onCreate() {
        if (id == null) id = UUID.randomUUID().toString();
        createdAt = Instant.now();
    }

    // Constructors
    protected Order() {}

    public Order(String customerEmail, String productName, BigDecimal amount) {
        this.customerEmail = customerEmail;
        this.productName = productName;
        this.amount = amount;
    }

    // Getters
    public String getId()             { return id; }
    public String getCustomerEmail()  { return customerEmail; }
    public String getProductName()    { return productName; }
    public BigDecimal getAmount()     { return amount; }
    public OrderStatus getStatus()    { return status; }
    public Instant getCreatedAt()     { return createdAt; }

    public void setStatus(OrderStatus status) { this.status = status; }

    public enum OrderStatus { CREATED, PAID, SHIPPED, CANCELLED }
}
