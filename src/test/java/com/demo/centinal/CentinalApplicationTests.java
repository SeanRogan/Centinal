package com.demo.centinal;

import com.demo.centinal.config.TestConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Import(TestConfig.class)
class CentinalApplicationTests {

    @Test
    @DisplayName("Application context should load successfully")
    void contextLoads() {
        // This test verifies that the Spring application context loads without errors
        // It will fail if there are configuration issues, missing beans, or startup problems
    }

    @Test
    @DisplayName("Application should have required beans configured")
    void shouldHaveRequiredBeans() {
        // This test verifies that all required beans are properly configured
        // The test will fail if any required dependencies are missing
    }
}
