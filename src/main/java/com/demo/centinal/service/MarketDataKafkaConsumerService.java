package com.demo.centinal.service;

import com.demo.centinal.entities.MarketData;
import com.demo.centinal.model.MarketDataEvent;
import com.demo.centinal.repository.MarketDataRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Service for consuming market data events from Kafka and persisting to TimescaleDB.
 * Handles async processing with manual acknowledgment for reliability.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MarketDataKafkaConsumerService {

    private final MarketDataRepository marketDataRepository;
    private final ObjectMapper objectMapper;

    /**
     * Consumes market data events from Kafka and processes them asynchronously.
     * Uses manual acknowledgment to ensure message processing reliability.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.market-data:market-data}",
            groupId = "${spring.kafka.consumer.group-id:market-data-consumer}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeMarketDataEvent(
            @Payload MarketDataEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            Acknowledgment acknowledgment) throws JsonProcessingException {

        try {
            log.debug("Received market data event from Kafka: topic={}, partition={}, timestamp={}",
                    topic, partition, timestamp);

            processMarketDataMessage(event.getMessage());

            // Acknowledge successful processing
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process market data event from Kafka: {}", event, e);
            // Don't acknowledge - this will trigger retry logic
            throw e;
        }
    }

    /**
     * Processes the market data message and saves to TimescaleDB.
     * Reuses the existing logic from MarketDataStreamingService.
     */
    private void processMarketDataMessage(String message) throws JsonProcessingException {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);

            // Handle different message types
            String type = jsonNode.path("type").asText();

            switch (type) {
                case "ticker" -> processTickerMessage(jsonNode);
                case "heartbeat" -> log.debug("Received heartbeat from Kafka: {}", message);
                case "subscriptions" -> log.info("Subscription confirmed from Kafka: {}", message);
                case "error" -> log.error("WebSocket error from Kafka: {}", message);
                default -> log.debug("Unhandled message type '{}' from Kafka: {}", type, message);
            }

        } catch (Exception e) {
            log.error("Failed to process market data message from Kafka: {}", message, e);
            throw e;
            // Rethrow to prevent acknowledgment
        }
    }

    /**
     * Processes a ticker message and saves to TimescaleDB.
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
            log.debug("Saved market data from Kafka for {}: price={}", productId, priceStr);

        } catch (Exception e) {
            log.error("Failed to process ticker message from Kafka: {}", tickerNode.toString(), e);
            throw e; // Rethrow to prevent acknowledgment
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