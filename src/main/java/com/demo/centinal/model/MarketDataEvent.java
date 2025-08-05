package com.demo.centinal.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *  * Event class for market data messages received from WebSocket.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarketDataEvent {

    private String message;
    private String source;
    private long timestamp;

    public MarketDataEvent(String message, String source) {
        this.message = message;
        this.source = source;
        this.timestamp = System.currentTimeMillis();
    }
}
