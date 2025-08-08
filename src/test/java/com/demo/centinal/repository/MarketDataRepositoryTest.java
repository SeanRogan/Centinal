package com.demo.centinal.repository;

import com.demo.centinal.config.TestConfig;
import com.demo.centinal.entities.MarketData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Import(TestConfig.class)
@Transactional
class MarketDataRepositoryTest {

    @Autowired
    private MarketDataRepository marketDataRepository;

    private MarketData sampleMarketData;

    @BeforeEach
    void setUp() {
        marketDataRepository.deleteAll();
        
        sampleMarketData = MarketData.builder()
            .timestamp(Instant.now())
            .symbol("BTC-USD")
            .exchange("coinbase")
            .price(new BigDecimal("50000.00"))
            .volume(new BigDecimal("1000.5"))
            .bid(new BigDecimal("49999.00"))
            .ask(new BigDecimal("50001.00"))
            .high24h(new BigDecimal("51000.00"))
            .low24h(new BigDecimal("49000.00"))
            .open24h(new BigDecimal("49500.00"))
            .rawData("{\"type\":\"ticker\",\"product_id\":\"BTC-USD\"}")
            .build();
    }

    @Nested
    @DisplayName("Basic CRUD Operations")
    class CrudOperationTests {

        @Test
        @DisplayName("Should save market data successfully")
        void shouldSaveMarketData() {
            // When
            MarketData savedData = marketDataRepository.save(sampleMarketData);

            // Then
            assertNotNull(savedData.getId());
            assertEquals("BTC-USD", savedData.getSymbol());
            assertEquals("coinbase", savedData.getExchange());
            assertEquals(new BigDecimal("50000.00"), savedData.getPrice());
        }

        @Test
        @DisplayName("Should find market data by ID")
        void shouldFindMarketDataById() {
            // Given
            MarketData savedData = marketDataRepository.save(sampleMarketData);

            // When
            MarketData foundData = marketDataRepository.findById(savedData.getId()).orElse(null);

            // Then
            assertNotNull(foundData);
            assertEquals(savedData.getId(), foundData.getId());
            assertEquals("BTC-USD", foundData.getSymbol());
        }

        @Test
        @DisplayName("Should update market data")
        void shouldUpdateMarketData() {
            // Given
            MarketData savedData = marketDataRepository.save(sampleMarketData);
            savedData.setPrice(new BigDecimal("51000.00"));

            // When
            MarketData updatedData = marketDataRepository.save(savedData);

            // Then
            assertEquals(new BigDecimal("51000.00"), updatedData.getPrice());
        }

        @Test
        @DisplayName("Should delete market data")
        void shouldDeleteMarketData() {
            // Given
            MarketData savedData = marketDataRepository.save(sampleMarketData);

            // When
            marketDataRepository.deleteById(savedData.getId());

            // Then
            assertFalse(marketDataRepository.findById(savedData.getId()).isPresent());
        }
    }

    @Nested
    @DisplayName("Time-Series Query Operations")
    class TimeSeriesQueryTests {

        @BeforeEach
        void setUpTestData() {
            // Create test data with different timestamps
            Instant now = Instant.now();
            Instant oneHourAgo = now.minusSeconds(3600);
            Instant twoHoursAgo = now.minusSeconds(7200);

            MarketData data1 = MarketData.builder()
                .timestamp(now)
                .symbol("BTC-USD")
                .exchange("coinbase")
                .price(new BigDecimal("50000.00"))
                .build();

            MarketData data2 = MarketData.builder()
                .timestamp(oneHourAgo)
                .symbol("BTC-USD")
                .exchange("coinbase")
                .price(new BigDecimal("49000.00"))
                .build();

            MarketData data3 = MarketData.builder()
                .timestamp(twoHoursAgo)
                .symbol("ETH-USD")
                .exchange("coinbase")
                .price(new BigDecimal("3000.00"))
                .build();

            marketDataRepository.saveAll(List.of(data1, data2, data3));
        }

        @Test
        @DisplayName("Should find market data with price above threshold in time range")
        void shouldFindMarketDataWithPriceAboveThreshold() {
            // Given
            Instant endTime = Instant.now();
            Instant startTime = endTime.minusSeconds(7200); // 2 hours ago
            BigDecimal threshold = new BigDecimal("49500.00");

            // When
            List<MarketData> results = marketDataRepository.findBySymbolAndPriceGreaterThanAndTimestampBetween(
                "BTC-USD", threshold, startTime, endTime);

            // Then
            assertFalse(results.isEmpty());
            results.forEach(data -> {
                assertEquals("BTC-USD", data.getSymbol());
                assertTrue(data.getPrice().compareTo(threshold) > 0);
                assertTrue(data.getTimestamp().isAfter(startTime) || data.getTimestamp().equals(startTime));
                assertTrue(data.getTimestamp().isBefore(endTime) || data.getTimestamp().equals(endTime));
            });
        }

        @Test
        @DisplayName("Should return empty list when no data matches criteria")
        void shouldReturnEmptyListWhenNoMatch() {
            // Given
            Instant endTime = Instant.now();
            Instant startTime = endTime.minusSeconds(3600); // 1 hour ago
            BigDecimal threshold = new BigDecimal("60000.00"); // Very high threshold

            // When
            List<MarketData> results = marketDataRepository.findBySymbolAndPriceGreaterThanAndTimestampBetween(
                "BTC-USD", threshold, startTime, endTime);

            // Then
            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Should get OHLC data for symbol in time range")
        void shouldGetOHLCData() {
            // Given
            Instant endTime = Instant.now();
            Instant startTime = endTime.minusSeconds(7200); // 2 hours ago

            // When
            List<Object[]> results = marketDataRepository.getOHLCData("BTC-USD", startTime, endTime);

            // Then
            assertFalse(results.isEmpty());
            Object[] ohlcData = results.get(0);
            assertEquals("BTC-USD", ohlcData[0]); // symbol
            assertNotNull(ohlcData[1]); // low
            assertNotNull(ohlcData[2]); // high
            assertNotNull(ohlcData[3]); // avg_price
            assertNotNull(ohlcData[4]); // count
        }
    }

    @Nested
    @DisplayName("Data Validation")
    class DataValidationTests {

        @Test
        @DisplayName("Should handle null values in market data")
        void shouldHandleNullValues() {
            // Given
            MarketData dataWithNulls = MarketData.builder()
                .timestamp(Instant.now())
                .symbol("BTC-USD")
                .exchange("coinbase")
                .price(new BigDecimal("50000.00"))
                .volume(null)
                .bid(null)
                .ask(null)
                .high24h(null)
                .low24h(null)
                .open24h(null)
                .rawData(null)
                .build();

            // When
            MarketData savedData = marketDataRepository.save(dataWithNulls);

            // Then
            assertNotNull(savedData.getId());
            assertEquals("BTC-USD", savedData.getSymbol());
            assertNull(savedData.getVolume());
            assertNull(savedData.getBid());
            assertNull(savedData.getAsk());
        }

        @Test
        @DisplayName("Should handle large decimal values")
        void shouldHandleLargeDecimalValues() {
            // Given
            MarketData dataWithLargeValues = MarketData.builder()
                .timestamp(Instant.now())
                .symbol("BTC-USD")
                .exchange("coinbase")
                .price(new BigDecimal("999999999.99999999"))
                .volume(new BigDecimal("123456789.12345678"))
                .build();

            // When
            MarketData savedData = marketDataRepository.save(dataWithLargeValues);

            // Then
            assertNotNull(savedData.getId());
            assertEquals(new BigDecimal("999999999.99999999"), savedData.getPrice());
            assertEquals(new BigDecimal("123456789.12345678"), savedData.getVolume());
        }
    }

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle bulk insert operations")
        void shouldHandleBulkInsert() {
            // Given
            List<MarketData> bulkData = List.of(
                MarketData.builder().timestamp(Instant.now()).symbol("BTC-USD").exchange("coinbase").price(new BigDecimal("50000.00")).build(),
                MarketData.builder().timestamp(Instant.now()).symbol("ETH-USD").exchange("coinbase").price(new BigDecimal("3000.00")).build(),
                MarketData.builder().timestamp(Instant.now()).symbol("ADA-USD").exchange("coinbase").price(new BigDecimal("0.50")).build()
            );

            // When
            List<MarketData> savedData = marketDataRepository.saveAll(bulkData);

            // Then
            assertEquals(3, savedData.size());
            savedData.forEach(data -> assertNotNull(data.getId()));
        }

        @Test
        @DisplayName("Should handle concurrent access")
        void shouldHandleConcurrentAccess() throws InterruptedException {
            // Given
            int threadCount = 5;
            int operationsPerThread = 10;
            Thread[] threads = new Thread[threadCount];

            // When
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < operationsPerThread; j++) {
                        MarketData data = MarketData.builder()
                            .timestamp(Instant.now())
                            .symbol("BTC-USD")
                            .exchange("coinbase")
                            .price(new BigDecimal("50000.00"))
                            .build();
                        marketDataRepository.save(data);
                    }
                });
                threads[i].start();
            }

            // Wait for all threads to complete
            for (Thread thread : threads) {
                thread.join();
            }

            // Then
            long totalCount = marketDataRepository.count();
            assertEquals(threadCount * operationsPerThread, totalCount);
        }
    }
} 