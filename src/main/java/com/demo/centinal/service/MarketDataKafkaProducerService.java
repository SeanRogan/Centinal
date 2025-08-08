package com.demo.centinal.service;

import com.demo.centinal.model.MarketDataEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class MarketDataKafkaProducerService {

    private final KafkaTemplate<String, MarketDataEvent> template;

    @Value("${spring.kafka.topics.market-data:market-data}")
    private String topic;


    public void publishEvent(MarketDataEvent event) {
        try {
            String partitionKey = generatePartitionKey(event);

            CompletableFuture<SendResult<String, MarketDataEvent>> future = template.send(topic, partitionKey, event);
            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to send market data event to Kafka: {}", event, throwable);
                    log.error(throwable.getMessage(), throwable);
                } else {
                    log.debug("Successfully sent market data event: topic={}, partition={}, offset={}",
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch ( Exception e ) {
            log.error("An error occurred when publishing to topic", e);
            e.printStackTrace();
        }
    }

    /**
     * Generates a partition key for the event.
     * This ensures related market data (same symbol) goes to the same partition.
     */
    private String generatePartitionKey(MarketDataEvent event) {
        // Extract symbol from message for partitioning
        try {
            // Simple key generation - TODO extract actual symbol from JSON
            return event.getSource() + "-" + System.currentTimeMillis() % 10;
        } catch (Exception e) {
            return event.getSource();
        }
    }
}
