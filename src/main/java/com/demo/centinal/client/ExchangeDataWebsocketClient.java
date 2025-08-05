package com.demo.centinal.client;

/**
 * Blueprint interface for WebSocket clients streaming market data from exchanges.
 * Implementations should handle connection lifecycle and message streaming.
 */
public interface ExchangeDataWebsocketClient {
    /**
     * Connects to the exchange WebSocket feed and starts streaming data.
     * @throws Exception if connection fails
     */
    void connect() throws Exception;

    /**
     * Disconnects from the exchange WebSocket feed.
     */
    void disconnect();

    /**
     * Checks if the client is currently connected to the WebSocket feed.
     * @return true if connected, false otherwise
     */
    boolean isConnected();

    /**
     * Sends a custom message to the WebSocket server (e.g., for subscribing/unsubscribing channels).
     * @param message the message to send
     */
    void sendMessage(String message);
} 