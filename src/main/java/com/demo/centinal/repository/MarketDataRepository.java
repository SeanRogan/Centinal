package com.demo.centinal.repository;

import com.demo.centinal.entities.MarketData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.security.authentication.jaas.JaasPasswordCallbackHandler;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Repository
public interface MarketDataRepository extends JpaRepository<MarketData, Long> {
    /**
     * Find market data with price above threshold in time range.
     */
    List<MarketData> findBySymbolAndPriceGreaterThanAndTimestampBetween(
            String symbol, BigDecimal price, Instant startTime, Instant endTime);

    /**
     * Custom query for time-series aggregation (e.g., OHLC data).
     */
    @Query("""
        SELECT m.symbol, 
               MIN(m.price) as low, 
               MAX(m.price) as high, 
               AVG(m.price) as avg_price,
               COUNT(m.id) as count
        FROM MarketData m 
        WHERE m.symbol = :symbol 
        AND m.timestamp BETWEEN :startTime AND :endTime
        GROUP BY m.symbol
        """)
    List<Object[]> getOHLCData(@Param("symbol") String symbol,
                               @Param("startTime") Instant startTime,
                               @Param("endTime") Instant endTime);

}
