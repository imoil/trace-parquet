package com.samsung.ees.infra.api.dataprovider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Oracle to Parquet export service.
 */
@SpringBootApplication
public class TradeParquetApplication {
    public static void main(String[] args) {
        SpringApplication.run(TradeParquetApplication.class, args);
    }
}
