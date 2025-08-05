package com.demo.centinal.client;

import com.demo.centinal.model.MarketDataEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * WebSocket client implementation for Coinbase Exchange.
 * Streams market data from Coinbase WebSocket feed with authentication support.
 */
@Data
@Service
@Slf4j
public class CoinbaseWebsocketClient implements ExchangeDataWebsocketClient {
    private WebSocketClient webSocketClient;
    private boolean connected = false;
    private static final String OPEN_COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com";
    private static final String PRIVATE_COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com";
    private List<String> subscribedSymbols = new ArrayList<>();
    private final ApplicationEventPublisher eventPublisher;
    private boolean authenticated = false;
    // Inject API credentials from application properties or environment variables
    @Value("${coinbase.api.key:}")
    private String apiKey;
    @Value("${coinbase.api.secret:}")
    private String apiSecret;
    @Value("${coinbase.api.passphrase:}")
    private String passphrase;
    
    @Autowired
    public CoinbaseWebsocketClient(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    /**
     * Connects to the Coinbase WebSocket feed and starts streaming data.
     * Authenticates if API credentials are provided.
     */
    @Override
    public void connect() throws URISyntaxException {
        connect(subscribedSymbols);
    }
    
    /**
     * Connects to the Coinbase WebSocket feed with specific symbols.
     * @param symbols list of symbols to subscribe to
     */
    public void connect(List<String> symbols) throws URISyntaxException {
        if (connected) return;
        this.subscribedSymbols = symbols != null ? symbols : new ArrayList<>();

        webSocketClient = new WebSocketClient(new URI(OPEN_COINBASE_WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                // Subscribe to ticker channel for the specified symbols
                String subscribeMessage = buildSubscribeMessage(subscribedSymbols);
                send(subscribeMessage);
                connected = true;
                log.info("Connected to Coinbase WebSocket and subscribed to symbols: {}", subscribedSymbols);
            }
            @Override
            public void onMessage(String message) {
                try {
                    MarketDataEvent event = new MarketDataEvent(message, "coinbase");
                    eventPublisher.publishEvent(event);
                } catch (Exception e) {
                    log.error("Failed to process WebSocket message: {}", message, e);
                }
            }
            @Override
            public void onClose(int code, String reason, boolean remote) {
                webSocketClient.close(code, reason);
                connected = false;
            }
            @Override
            public void onError(Exception ex) {
                connected = false;
                // TODO: Add error logging
            }
        };
        webSocketClient.connect();
    }


    private String buildSubscribeMessage(List<String> productIds) {
        if (authenticated) {
            return buildAuthenticatedSubscribeMessage(productIds);
        }

        // Convert list to JSON array format
        String productIdsJson = productIds.isEmpty() ? 
            "[\"BTC-USD\",\"ETH-USD\"]" : // Default symbols if none provided
            "[\"" + String.join("\",\"", productIds) + "\"]";
            
        return String.format("""
        {"type": "subscribe", "channels": [{ "name": "ticker", "product_ids": %s }]}
        """, productIdsJson);
    }
    /**
     * Builds an authenticated subscribe message for the Coinbase WebSocket feed.
     * @return JSON string for subscription with authentication fields
     */
    private String buildAuthenticatedSubscribeMessage(List<String> productIds) {
        try {
            String timestamp = String.valueOf(Instant.now().getEpochSecond());
            String method = "GET";
            String requestPath = "/users/self/verify";
            String prehash = timestamp + method + requestPath;
            String signature = generateSignature(prehash, apiSecret);
            
            // Convert list to JSON array format
            String productIdsJson = productIds.isEmpty() ? 
                "[\"BTC-USD\",\"ETH-USD\"]" : 
                "[\"" + String.join("\",\"", productIds) + "\"]";
                
            return String.format("""
            {"type": "subscribe",
             "channels": [{ "name": "ticker", "product_ids": %s }],
             "signature": "%s",
             "key": "%s",
             "passphrase": "%s",
             "timestamp": "%s"
            }
            """, productIdsJson, signature, apiKey, passphrase, timestamp);
        } catch (Exception e) {
            log.error("Failed to build authenticated subscribe message", e);
            throw new RuntimeException("Failed to build authenticated subscribe message", e);
        }
    }

    /**
     * Generates a base64-encoded HMAC SHA256 signature for authentication.
     * @param prehash the string to sign
     * @param secret the API secret
     * @return the base64-encoded signature
     */
    private String generateSignature(String prehash, String secret) throws Exception {
        SecretKeySpec secretKeySpec = new SecretKeySpec(Base64.getDecoder().decode(secret), "HmacSHA256");
        Mac sha256 = Mac.getInstance("HmacSHA256");
        sha256.init(secretKeySpec);
        byte[] hash = sha256.doFinal(prehash.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }

    /**
     * Disconnects from the Coinbase WebSocket feed.
     */
    @Override
    public void disconnect() {
        if (webSocketClient != null && connected) {
            try {
                webSocketClient.close();
                connected = false;
            } catch (Exception e) {
                log.error("Failed to close websocket client", e);
            }
        }
    }

    /**
     * Checks if the client is currently connected to the WebSocket feed.
     * @return true if connected, false otherwise
     */
    @Override
    public boolean isConnected() {
        return connected;
    }

    /**
     * Sends a custom message to the WebSocket server.
     * @param message the message to send
     */
    @Override
    public void sendMessage(String message) {
        if (webSocketClient != null && connected) {
            webSocketClient.send(message);
        }
    }
} 