package com.demo.centinal.service;

import com.demo.centinal.client.CoinbaseWebsocketClient;
import com.demo.centinal.entities.MarketData;
import com.demo.centinal.model.MarketDataEvent;
import com.demo.centinal.repository.MarketDataRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service for handling market data streaming from exchanges to TimescaleDB.
 * Processes incoming WebSocket messages and persists them to the database.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MarketDataStreamingService {
    
    private final MarketDataRepository marketDataRepository;
    private final CoinbaseWebsocketClient coinbaseClient;
    private final ObjectMapper objectMapper;
    
    @Value("${market.data.symbols:BTC-USD}")
    private List<String> assetSymbols;

    /**
     * Starts the market data streaming process.
     * Connects to Coinbase WebSocket and begins processing messages.
     */
    @Async
    public CompletableFuture<Void> startStreaming() {
        try {
            log.info("Starting market data streaming for symbols: {}", assetSymbols);
            coinbaseClient.connect();
            
            // Subscribe to default symbols
            subscribeToSymbols(assetSymbols);
            
            log.info("Market data streaming started successfully");
            return CompletableFuture.completedFuture(null);
            
        } catch (Exception e) {
            log.error("Failed to start market data streaming", e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * Stops the market data streaming process.
     */
    public void stopStreaming() {
        log.info("Stopping market data streaming");
        coinbaseClient.disconnect();
    }
    
    /**
     * Processes incoming market data message from WebSocket.
     * Parses the JSON message and persists to database.
     */
    @EventListener
    @Transactional
    public void handleMarketDataEvent(MarketDataEvent event) {
        processMarketDataMessage(event.getMessage());
    }

    public void processMarketDataMessage(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            
            // Handle different message types
            String type = jsonNode.path("type").asText();
            
            switch (type) {
                case "ticker" -> processTickerMessage(jsonNode);
                case "heartbeat" -> log.debug("Received heartbeat: {}", message);
                case "subscriptions" -> log.info("Subscription confirmed: {}", message);
                case "error" -> log.error("WebSocket error: {}", message);
                default -> log.debug("Unhandled message type '{}': {}", type, message);
            }
            
        } catch (Exception e) {
            log.error("Failed to process market data message: {}", message, e);
        }
    }
    
    /**
     * Processes a ticker message from Coinbase WebSocket.
     */
    private void processTickerMessage(JsonNode tickerNode) {
        try {
            String productId = tickerNode.path("product_id").asText();
            String priceStr = tickerNode.path("price").asText();
            String volumeStr = tickerNode.path("volume_24h").asText();
            String bidStr = tickerNode.path("bid").asText();
            String askStr = tickerNode.path("ask").asText();
            String highStr = tickerNode.path("high_24h").asText();
            String lowStr = tickerNode.path("low_24h").asText();
            String openStr = tickerNode.path("open_24h").asText();
            
            MarketData marketData = MarketData.builder()
                .timestamp(Instant.now())
                .symbol(productId)
                .exchange("coinbase")
                .price(parseBigDecimal(priceStr))
                .volume(parseBigDecimal(volumeStr))
                .bid(parseBigDecimal(bidStr))
                .ask(parseBigDecimal(askStr))
                .high24h(parseBigDecimal(highStr))
                .low24h(parseBigDecimal(lowStr))
                .open24h(parseBigDecimal(openStr))
                .rawData(tickerNode.toString())
                .build();
            
            marketDataRepository.save(marketData);
            log.debug("Saved market data for {}: price={}", productId, priceStr);
            
        } catch (Exception e) {
            log.error("Failed to process ticker message: {}", tickerNode.toString(), e);
        }
    }
    
    /**
     * Subscribes to specific symbols on the WebSocket.
     */
    private void subscribeToSymbols(List<String> symbols) {
        try {
            // Connect with the specified symbols
            coinbaseClient.connect(symbols);
            log.info("Subscribed to symbols: {}", symbols);
        } catch (Exception e) {
            log.error("Failed to subscribe to symbols: {}", symbols, e);
        }
    }
    
    /**
     * Safely parses a BigDecimal from string, returning null if parsing fails.
     */
    private BigDecimal parseBigDecimal(String value) {
        if (value == null || value.isEmpty() || "null".equals(value)) {
            return null;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            log.warn("Failed to parse BigDecimal from: {}", value);
            return null;
        }
    }
} 