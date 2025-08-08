package com.demo.centinal.service;

import com.demo.centinal.client.CoinbaseWebsocketClient;
import com.demo.centinal.entities.MarketData;
import com.demo.centinal.model.MarketDataEvent;
import com.demo.centinal.repository.MarketDataRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MarketDataStreamingServiceTest {

    @Mock
    private MarketDataRepository marketDataRepository;

    @Mock
    private CoinbaseWebsocketClient coinbaseClient;

    @Mock
    private ObjectMapper objectMapper;

    private MarketDataStreamingService streamingService;

    @BeforeEach
    void setUp() {
        streamingService = new MarketDataStreamingService(marketDataRepository, coinbaseClient, objectMapper);
        ReflectionTestUtils.setField(streamingService, "assetSymbols", List.of("BTC-USD", "ETH-USD"));
    }

    @Nested
    @DisplayName("Streaming Lifecycle")
    class StreamingLifecycleTests {

        @Test
        @DisplayName("Should start streaming successfully")
        void shouldStartStreaming() throws Exception {
            // Given
            doNothing().when(coinbaseClient).connect();
            doNothing().when(coinbaseClient).connect(anyList());

            // When
            CompletableFuture<Void> result = streamingService.startStreaming();

            // Then
            assertNotNull(result);
            verify(coinbaseClient).connect();
            verify(coinbaseClient).connect(anyList());
        }

        @Test
        @DisplayName("Should handle streaming start failure")
        void shouldHandleStreamingStartFailure() throws Exception {
            // Given
            doThrow(new RuntimeException("Connection failed")).when(coinbaseClient).connect();

            // When
            CompletableFuture<Void> result = streamingService.startStreaming();

            // Then
            assertNotNull(result);
            assertTrue(result.isCompletedExceptionally());
        }

        @Test
        @DisplayName("Should stop streaming successfully")
        void shouldStopStreaming() {
            // When
            streamingService.stopStreaming();

            // Then
            verify(coinbaseClient).disconnect();
        }
    }

    @Nested
    @DisplayName("Message Processing")
    class MessageProcessingTests {

        @Test
        @DisplayName("Should process ticker message correctly")
        void shouldProcessTickerMessage() throws Exception {
            // Given
            String tickerMessage = """
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
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            ObjectNode productIdNode = mock(ObjectNode.class);
            ObjectNode priceNode = mock(ObjectNode.class);
            ObjectNode volumeNode = mock(ObjectNode.class);
            ObjectNode bidNode = mock(ObjectNode.class);
            ObjectNode askNode = mock(ObjectNode.class);
            ObjectNode highNode = mock(ObjectNode.class);
            ObjectNode lowNode = mock(ObjectNode.class);
            ObjectNode openNode = mock(ObjectNode.class);
            
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(jsonNode.path("product_id")).thenReturn(productIdNode);
            when(jsonNode.path("price")).thenReturn(priceNode);
            when(jsonNode.path("volume_24h")).thenReturn(volumeNode);
            when(jsonNode.path("bid")).thenReturn(bidNode);
            when(jsonNode.path("ask")).thenReturn(askNode);
            when(jsonNode.path("high_24h")).thenReturn(highNode);
            when(jsonNode.path("low_24h")).thenReturn(lowNode);
            when(jsonNode.path("open_24h")).thenReturn(openNode);
            
            when(typeNode.asText()).thenReturn("ticker");
            when(productIdNode.asText()).thenReturn("BTC-USD");
            when(priceNode.asText()).thenReturn("50000.00");
            when(volumeNode.asText()).thenReturn("1000.5");
            when(bidNode.asText()).thenReturn("49999.00");
            when(askNode.asText()).thenReturn("50001.00");
            when(highNode.asText()).thenReturn("51000.00");
            when(lowNode.asText()).thenReturn("49000.00");
            when(openNode.asText()).thenReturn("49500.00");
            when(jsonNode.toString()).thenReturn(tickerMessage);

            // When
            streamingService.processMarketDataMessage(tickerMessage);

            // Then
            verify(marketDataRepository).save(any(MarketData.class));
        }

        @Test
        @DisplayName("Should handle heartbeat messages")
        void shouldHandleHeartbeatMessage() throws Exception {
            // Given
            String heartbeatMessage = """
                {
                    "type": "heartbeat",
                    "sequence": 12345,
                    "time": "2023-01-01T00:00:00.000Z"
                }
                """;
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(typeNode.asText()).thenReturn("heartbeat");

            // When
            streamingService.processMarketDataMessage(heartbeatMessage);

            // Then
            verify(marketDataRepository, never()).save(any(MarketData.class));
        }

        @Test
        @DisplayName("Should handle subscription confirmation")
        void shouldHandleSubscriptionConfirmation() throws Exception {
            // Given
            String subscriptionMessage = """
                {
                    "type": "subscriptions",
                    "channels": [
                        {
                            "name": "ticker",
                            "product_ids": ["BTC-USD", "ETH-USD"]
                        }
                    ]
                }
                """;
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(typeNode.asText()).thenReturn("subscriptions");

            // When
            streamingService.processMarketDataMessage(subscriptionMessage);

            // Then
            verify(marketDataRepository, never()).save(any(MarketData.class));
        }

        @Test
        @DisplayName("Should handle error messages")
        void shouldHandleErrorMessage() throws Exception {
            // Given
            String errorMessage = """
                {
                    "type": "error",
                    "message": "Invalid subscription"
                }
                """;
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(typeNode.asText()).thenReturn("error");

            // When
            streamingService.processMarketDataMessage(errorMessage);

            // Then
            verify(marketDataRepository, never()).save(any(MarketData.class));
        }

        @Test
        @DisplayName("Should handle malformed JSON")
        void shouldHandleMalformedJson() throws Exception {
            // Given
            String malformedMessage = "invalid json";
            when(objectMapper.readTree(anyString())).thenThrow(new RuntimeException("Invalid JSON"));

            // When
            streamingService.processMarketDataMessage(malformedMessage);

            // Then
            verify(marketDataRepository, never()).save(any(MarketData.class));
        }

        @Test
        @DisplayName("Should handle null or empty values in ticker data")
        void shouldHandleNullValuesInTickerData() throws Exception {
            // Given
            String tickerMessageWithNulls = """
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
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            ObjectNode productIdNode = mock(ObjectNode.class);
            ObjectNode priceNode = mock(ObjectNode.class);
            ObjectNode volumeNode = mock(ObjectNode.class);
            ObjectNode bidNode = mock(ObjectNode.class);
            ObjectNode askNode = mock(ObjectNode.class);
            ObjectNode highNode = mock(ObjectNode.class);
            ObjectNode lowNode = mock(ObjectNode.class);
            ObjectNode openNode = mock(ObjectNode.class);
            
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(jsonNode.path("product_id")).thenReturn(productIdNode);
            when(jsonNode.path("price")).thenReturn(priceNode);
            when(jsonNode.path("volume_24h")).thenReturn(volumeNode);
            when(jsonNode.path("bid")).thenReturn(bidNode);
            when(jsonNode.path("ask")).thenReturn(askNode);
            when(jsonNode.path("high_24h")).thenReturn(highNode);
            when(jsonNode.path("low_24h")).thenReturn(lowNode);
            when(jsonNode.path("open_24h")).thenReturn(openNode);
            
            when(typeNode.asText()).thenReturn("ticker");
            when(productIdNode.asText()).thenReturn("BTC-USD");
            when(priceNode.asText()).thenReturn("50000.00");
            when(volumeNode.asText()).thenReturn(null);
            when(bidNode.asText()).thenReturn("");
            when(askNode.asText()).thenReturn("null");
            when(highNode.asText()).thenReturn("51000.00");
            when(lowNode.asText()).thenReturn("49000.00");
            when(openNode.asText()).thenReturn("49500.00");
            when(jsonNode.toString()).thenReturn(tickerMessageWithNulls);

            // When
            streamingService.processMarketDataMessage(tickerMessageWithNulls);

            // Then
            verify(marketDataRepository).save(any(MarketData.class));
        }
    }

    @Nested
    @DisplayName("Event Handling")
    class EventHandlingTests {

        @Test
        @DisplayName("Should handle MarketDataEvent correctly")
        void shouldHandleMarketDataEvent() throws Exception {
            // Given
            String message = """
                {
                    "type": "ticker",
                    "product_id": "BTC-USD",
                    "price": "50000.00"
                }
                """;
            MarketDataEvent event = new MarketDataEvent(message, "coinbase");
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            ObjectNode productIdNode = mock(ObjectNode.class);
            ObjectNode priceNode = mock(ObjectNode.class);
            ObjectNode volumeNode = mock(ObjectNode.class);
            ObjectNode bidNode = mock(ObjectNode.class);
            ObjectNode askNode = mock(ObjectNode.class);
            ObjectNode highNode = mock(ObjectNode.class);
            ObjectNode lowNode = mock(ObjectNode.class);
            ObjectNode openNode = mock(ObjectNode.class);
            
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(jsonNode.path("product_id")).thenReturn(productIdNode);
            when(jsonNode.path("price")).thenReturn(priceNode);
            when(jsonNode.path("volume_24h")).thenReturn(volumeNode);
            when(jsonNode.path("bid")).thenReturn(bidNode);
            when(jsonNode.path("ask")).thenReturn(askNode);
            when(jsonNode.path("high_24h")).thenReturn(highNode);
            when(jsonNode.path("low_24h")).thenReturn(lowNode);
            when(jsonNode.path("open_24h")).thenReturn(openNode);
            
            when(typeNode.asText()).thenReturn("ticker");
            when(productIdNode.asText()).thenReturn("BTC-USD");
            when(priceNode.asText()).thenReturn("50000.00");
            when(volumeNode.asText()).thenReturn("1000.5");
            when(bidNode.asText()).thenReturn("49999.00");
            when(askNode.asText()).thenReturn("50001.00");
            when(highNode.asText()).thenReturn("51000.00");
            when(lowNode.asText()).thenReturn("49000.00");
            when(openNode.asText()).thenReturn("49500.00");
            when(jsonNode.toString()).thenReturn(message);

            // When
            streamingService.handleMarketDataEvent(event);

            // Then
            verify(marketDataRepository).save(any(MarketData.class));
        }
    }

    @Nested
    @DisplayName("Data Validation")
    class DataValidationTests {

        @Test
        @DisplayName("Should validate market data before saving")
        void shouldValidateMarketData() throws Exception {
            // Given
            String tickerMessage = """
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
            
            ObjectNode jsonNode = mock(ObjectNode.class);
            ObjectNode typeNode = mock(ObjectNode.class);
            ObjectNode productIdNode = mock(ObjectNode.class);
            ObjectNode priceNode = mock(ObjectNode.class);
            ObjectNode volumeNode = mock(ObjectNode.class);
            ObjectNode bidNode = mock(ObjectNode.class);
            ObjectNode askNode = mock(ObjectNode.class);
            ObjectNode highNode = mock(ObjectNode.class);
            ObjectNode lowNode = mock(ObjectNode.class);
            ObjectNode openNode = mock(ObjectNode.class);
            
            when(objectMapper.readTree(anyString())).thenReturn(jsonNode);
            when(jsonNode.path("type")).thenReturn(typeNode);
            when(jsonNode.path("product_id")).thenReturn(productIdNode);
            when(jsonNode.path("price")).thenReturn(priceNode);
            when(jsonNode.path("volume_24h")).thenReturn(volumeNode);
            when(jsonNode.path("bid")).thenReturn(bidNode);
            when(jsonNode.path("ask")).thenReturn(askNode);
            when(jsonNode.path("high_24h")).thenReturn(highNode);
            when(jsonNode.path("low_24h")).thenReturn(lowNode);
            when(jsonNode.path("open_24h")).thenReturn(openNode);
            
            when(typeNode.asText()).thenReturn("ticker");
            when(productIdNode.asText()).thenReturn("BTC-USD");
            when(priceNode.asText()).thenReturn("50000.00");
            when(volumeNode.asText()).thenReturn("1000.5");
            when(bidNode.asText()).thenReturn("49999.00");
            when(askNode.asText()).thenReturn("50001.00");
            when(highNode.asText()).thenReturn("51000.00");
            when(lowNode.asText()).thenReturn("49000.00");
            when(openNode.asText()).thenReturn("49500.00");
            when(jsonNode.toString()).thenReturn(tickerMessage);

            ArgumentCaptor<MarketData> marketDataCaptor = ArgumentCaptor.forClass(MarketData.class);

            // When
            streamingService.processMarketDataMessage(tickerMessage);

            // Then
            verify(marketDataRepository).save(marketDataCaptor.capture());
            MarketData savedData = marketDataCaptor.getValue();
            
            assertEquals("BTC-USD", savedData.getSymbol());
            assertEquals("coinbase", savedData.getExchange());
            assertEquals(new BigDecimal("50000.00"), savedData.getPrice());
            assertEquals(new BigDecimal("1000.5"), savedData.getVolume());
            assertEquals(new BigDecimal("49999.00"), savedData.getBid());
            assertEquals(new BigDecimal("50001.00"), savedData.getAsk());
            assertEquals(new BigDecimal("51000.00"), savedData.getHigh24h());
            assertEquals(new BigDecimal("49000.00"), savedData.getLow24h());
            assertEquals(new BigDecimal("49500.00"), savedData.getOpen24h());
            assertNotNull(savedData.getTimestamp());
            // Note: createdAt is set by @PrePersist which only runs on actual persistence
            // In tests with mocked repository, this field will be null
        }
    }
} 