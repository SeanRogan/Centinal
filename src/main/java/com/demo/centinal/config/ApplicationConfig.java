package com.demo.centinal.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Application configuration for market data streaming service.
 * Enables async processing and transaction management.
 */
@Configuration
@EnableAsync
@EnableTransactionManagement
public class ApplicationConfig {
    
    /**
     * Configures ObjectMapper for JSON processing with Java 8 time module.
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
} 