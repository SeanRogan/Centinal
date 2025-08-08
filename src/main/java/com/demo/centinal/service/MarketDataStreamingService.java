package com.demo.centinal.service;

import com.demo.centinal.client.CoinbaseWebsocketClient;
import com.demo.centinal.repository.MarketDataRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

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
     * Stops the market data streaming process.
     */
    public void stopStreaming() {
        log.info("Stopping market data streaming");
        coinbaseClient.disconnect();
    }
} 