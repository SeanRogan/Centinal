package com.demo.centinal;

import com.demo.centinal.service.MarketDataStreamingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Main application class for the Centinal market data streaming service.
 * Automatically starts streaming market data from Coinbase to TimescaleDB.
 */
@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class CentinalApplication {
    
    private final MarketDataStreamingService streamingService;

    public static void main(String[] args) {
        SpringApplication.run(CentinalApplication.class, args);
    }
    
    /**
     * CommandLineRunner to start market data streaming when the application starts.
     */
    @Bean
    public CommandLineRunner startStreaming() {
        return args -> {
            log.info("Starting Centinal market data streaming service...");
            
            // Start the streaming service asynchronously
            streamingService.startStreaming()
                .thenRun(() -> log.info("Market data streaming started successfully"))
                .exceptionally(throwable -> {
                    log.error("Failed to start market data streaming", throwable);
                    return null;
                });
        };
    }
}
