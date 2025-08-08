package com.demo.centinal;

import com.demo.centinal.config.TestConfig;
import com.demo.centinal.entities.MarketData;
import com.demo.centinal.model.MarketDataEvent;
import com.demo.centinal.repository.MarketDataRepository;
import com.demo.centinal.service.MarketDataStreamingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Import(TestConfig.class)
@TestPropertySource(properties = {
    "market.data.symbols=BTC-USD,ETH-USD",
    "coinbase.api.key=",
    "coinbase.api.secret=",
    "coinbase.api.passphrase="
})
class ExchangeApiIntegrationTest {

    @Autowired
    private MarketDataStreamingService streamingService;

    @Autowired
    private MarketDataRepository marketDataRepository;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        marketDataRepository.deleteAll();
    }

    @Nested
    @DisplayName("Market Data Processing Integration")
    class MarketDataProcessingIntegrationTests {

        @Test
        @DisplayName("Should process and persist market data end-to-end")
        void shouldProcessAndPersistMarketData() throws Exception {
            // Given
            String tickerMessage = """
                {
                    "type": "ticker",
                    "product_id": "ETH-USD",
                    "price": "3000.00",
                    "volume_24h": "500.25",
                    "bid": "2999.50",
                    "ask": "3000.50",
                    "high_24h": "3100.00",
                    "low_24h": "2900.00",
                    "open_24h": "2950.00"
                }
                """;

            // When
            streamingService.processMarketDataMessage(tickerMessage);

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertFalse(savedData.isEmpty());
            
            MarketData latestData = savedData.get(savedData.size() - 1);
            assertEquals("ETH-USD", latestData.getSymbol());
            assertEquals("coinbase", latestData.getExchange());
            assertEquals(new BigDecimal("3000.00"), latestData.getPrice());
            assertEquals(new BigDecimal("500.25"), latestData.getVolume());
            assertEquals(new BigDecimal("2999.50"), latestData.getBid());
            assertEquals(new BigDecimal("3000.50"), latestData.getAsk());
        }

        @Test
        @DisplayName("Should handle multiple market data messages")
        void shouldHandleMultipleMarketDataMessages() throws Exception {
            // Given
            List<String> messages = List.of(
                """
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "50000.00"
                }
                """,
                """
                {
                    "type": "ticker",
                    "product_id": "ETH-USD",
                    "price": "3000.00"
                }
                """,
                """
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "50100.00"
                }
                """
            );

            // When
            for (String message : messages) {
                streamingService.processMarketDataMessage(message);
            }

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertEquals(3, savedData.size());
            
            // Verify different symbols were saved
            long btcCount = savedData.stream()
                .filter(data -> "BTC-USD".equals(data.getSymbol()))
                .count();
            long ethCount = savedData.stream()
                .filter(data -> "ETH-USD".equals(data.getSymbol()))
                .count();
            
            assertEquals(2, btcCount);
            assertEquals(1, ethCount);
        }

        @Test
        @DisplayName("Should handle non-ticker messages without persisting")
        void shouldHandleNonTickerMessages() throws Exception {
            // Given
            List<String> nonTickerMessages = List.of(
                """
                {
                    "type": "heartbeat",
                    "sequence": 12345,
                    "time": "2023-01-01T00:00:00.000Z"
                }
                """,
                """
                {
                    "type": "subscriptions",
                    "channels": [
                        {
                            "name": "ticker",
                            "product_ids": ["BTC-USD"]
                        }
                    ]
                }
                """,
                """
                {
                    "type": "error",
                    "message": "Invalid subscription"
                }
                """
            );

            // When
            for (String message : nonTickerMessages) {
                streamingService.processMarketDataMessage(message);
            }

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertTrue(savedData.isEmpty());
        }
    }

    @Nested
    @DisplayName("Event Processing Integration")
    class EventProcessingIntegrationTests {

        @Test
        @DisplayName("Should process market data events end-to-end")
        void shouldProcessMarketDataEventsEndToEnd() throws Exception {
            // Given
            String marketDataMessage = """
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "50000.00",
                    "volume_24h": "1000.5",
                    "bid": "49999.00",
                    "ask": "50001.00",
                    "high_24h": "51000.00",
                    "low_24h": "49000.00",
                    "open_24h": "49500.00"
                }
                """;

            // When
            MarketDataEvent event = new MarketDataEvent(marketDataMessage, "coinbase");
            eventPublisher.publishEvent(event);

            // Wait for async processing
            Thread.sleep(100);

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertFalse(savedData.isEmpty());
            
            MarketData data = savedData.get(0);
            assertEquals("BTC-USD", data.getSymbol());
            assertEquals("coinbase", data.getExchange());
            assertEquals(new BigDecimal("50000.00"), data.getPrice());
            assertEquals(new BigDecimal("1000.5"), data.getVolume());
        }

        @Test
        @DisplayName("Should handle multiple concurrent events")
        void shouldHandleMultipleConcurrentEvents() throws Exception {
            // Given
            int eventCount = 10;
            List<MarketDataEvent> events = new java.util.ArrayList<>();
            
            for (int i = 0; i < eventCount; i++) {
                String message = String.format("""
                    {
                        "type": "ticker",
                        "product_id": "BTC-USD",
                        "price": "%d.00"
                    }
                    """, 50000 + i);
                events.add(new MarketDataEvent(message, "coinbase"));
            }

            // When
            events.forEach(eventPublisher::publishEvent);

            // Wait for async processing
            Thread.sleep(200);

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertEquals(eventCount, savedData.size());
            
            // Verify all events were processed
            for (int i = 0; i < savedData.size(); i++) {
                MarketData data = savedData.get(i);
                assertEquals("BTC-USD", data.getSymbol());
                assertEquals(new BigDecimal(50000 + i + ".00"), data.getPrice());
            }
        }
    }

    @Nested
    @DisplayName("Data Persistence Integration")
    class DataPersistenceIntegrationTests {

        @Test
        @DisplayName("Should persist market data with all fields")
        void shouldPersistMarketDataWithAllFields() throws Exception {
            // Given
            String completeTickerMessage = """
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "50000.00",
                    "volume_24h": "1000.5",
                    "bid": "49999.00",
                    "ask": "50001.00",
                    "high_24h": "51000.00",
                    "low_24h": "49000.00",
                    "open_24h": "49500.00"
                }
                """;

            // When
            streamingService.processMarketDataMessage(completeTickerMessage);

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertFalse(savedData.isEmpty());
            
            MarketData data = savedData.get(0);
            assertEquals("BTC-USD", data.getSymbol());
            assertEquals("coinbase", data.getExchange());
            assertEquals(new BigDecimal("50000.00"), data.getPrice());
            assertEquals(new BigDecimal("1000.5"), data.getVolume());
            assertEquals(new BigDecimal("49999.00"), data.getBid());
            assertEquals(new BigDecimal("50001.00"), data.getAsk());
            assertEquals(new BigDecimal("51000.00"), data.getHigh24h());
            assertEquals(new BigDecimal("49000.00"), data.getLow24h());
            assertEquals(new BigDecimal("49500.00"), data.getOpen24h());
            assertNotNull(data.getRawData());
            assertNotNull(data.getTimestamp());
            assertNotNull(data.getCreatedAt());
        }

        @Test
        @DisplayName("Should handle malformed JSON gracefully")
        void shouldHandleMalformedJson() {
            // Given
            String malformedMessage = "invalid json message";

            // When & Then
            assertDoesNotThrow(() -> {
                streamingService.processMarketDataMessage(malformedMessage);
            });

            // Verify no data was persisted
            List<MarketData> savedData = marketDataRepository.findAll();
            assertTrue(savedData.isEmpty());
        }

        @Test
        @DisplayName("Should handle null values in ticker data")
        void shouldHandleNullValuesInTickerData() throws Exception {
            // Given
            String tickerWithNulls = """
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "50000.00",
                    "volume_24h": null,
                    "bid": "",
                    "ask": "null",
                    "high_24h": "51000.00",
                    "low_24h": "49000.00",
                    "open_24h": "49500.00"
                }
                """;

            // When
            streamingService.processMarketDataMessage(tickerWithNulls);

            // Then
            List<MarketData> savedData = marketDataRepository.findAll();
            assertFalse(savedData.isEmpty());
            
            MarketData data = savedData.get(0);
            assertEquals("BTC-USD", data.getSymbol());
            assertEquals(new BigDecimal("50000.00"), data.getPrice());
            assertNull(data.getVolume());
            assertNull(data.getBid());
            assertNull(data.getAsk());
        }
    }

    @Nested
    @DisplayName("Error Handling Integration")
    class ErrorHandlingIntegrationTests {

        @Test
        @DisplayName("Should handle malformed data responses")
        void shouldHandleMalformedDataResponses() {
            // Given
            List<String> malformedMessages = List.of(
                "invalid json",
                "{}",
                "{\"type\": \"unknown\"}",
                "{\"type\": \"ticker\"}", // missing required fields
                "{\"type\": \"ticker\", \"product_id\": \"BTC-USD\"}" // missing price
            );

            // When & Then
            for (String message : malformedMessages) {
                assertDoesNotThrow(() -> {
                    streamingService.processMarketDataMessage(message);
                });
            }
        }
    }
} 