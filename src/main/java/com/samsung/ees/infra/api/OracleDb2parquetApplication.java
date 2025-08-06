package com.samsung.ees.infra.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Oracle to Parquet export service.
 */
@SpringBootApplication
public class OracleDb2parquetApplication {

    public static void main(String[] args) {
        SpringApplication.run(OracleDb2parquetApplication.class, args);
    }

}
