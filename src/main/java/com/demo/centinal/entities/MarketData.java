package com.demo.centinal.entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entity for storing market data in TimescaleDB.
 * Optimized for time-series data with proper indexing and partitioning.
 */
@Entity
@Table(name = "market_data", indexes = {
    @Index(name = "idx_market_data_timestamp", columnList = "timestamp"),
    @Index(name = "idx_market_data_symbol", columnList = "symbol"),
    @Index(name = "idx_market_data_exchange", columnList = "exchange")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MarketData {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;
    
    @Column(name = "symbol", nullable = false, length = 20)
    private String symbol;
    
    @Column(name = "exchange", nullable = false, length = 50)
    private String exchange;
    
    @Column(name = "price", precision = 20, scale = 8)
    private BigDecimal price;
    
    @Column(name = "volume", precision = 20, scale = 8)
    private BigDecimal volume;
    
    @Column(name = "bid", precision = 20, scale = 8)
    private BigDecimal bid;
    
    @Column(name = "ask", precision = 20, scale = 8)
    private BigDecimal ask;
    
    @Column(name = "high_24h", precision = 20, scale = 8)
    private BigDecimal high24h;
    
    @Column(name = "low_24h", precision = 20, scale = 8)
    private BigDecimal low24h;
    
    @Column(name = "open_24h", precision = 20, scale = 8)
    private BigDecimal open24h;
    
    @Column(name = "raw_data", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String rawData;
    
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    
    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
} 