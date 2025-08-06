package com.samsung.ees.infra.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.samsung.ees.infra.api.model.SensorData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class ParquetConversionServiceTest {

    private ParquetConversionService parquetConversionService;

    @BeforeEach
    void setUp() {
        // Initialize the service with a standard ObjectMapper
        parquetConversionService = new ParquetConversionService(new ObjectMapper());
    }

    // Helper method to create GZIP compressed data from a string
    private byte[] createGzipData(String content) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(content.getBytes("UTF-8"));
        }
        return byteStream.toByteArray();
    }

    @Test
    void convertToParquet_shouldProduceValidParquetFile(@TempDir java.nio.file.Path tempDir) throws IOException {
        // 1. Arrange: Create mock data
        LocalDateTime now = LocalDateTime.now();
        SensorData data1 = new SensorData(now, now.plusHours(1), 1L, createGzipData("{\"value\": 100, \"status\": \"OK\"}"));
        SensorData data2 = new SensorData(now.plusMinutes(1), now.plusHours(1).plusMinutes(1), 2L, createGzipData("{\"value\": 200, \"status\": \"WARN\"}"));
        Flux<SensorData> sensorDataFlux = Flux.just(data1, data2);

        // 2. Act: Call the service method
        StepVerifier.create(parquetConversionService.convertToParquet(sensorDataFlux))
                .assertNext(parquetBytes -> {
                    // 3. Assert: Verify the output
                    assertNotNull(parquetBytes);
                    assertTrue(parquetBytes.length > 0);

                    // To truly verify, write bytes to a temp file and read it back with a Parquet reader
                    File tempParquetFile = null;
                    try {
                        tempParquetFile = tempDir.resolve("test.parquet").toFile();
                        try (FileOutputStream fos = new FileOutputStream(tempParquetFile)) {
                            fos.write(parquetBytes);
                        }

                        List<GenericRecord> records = readParquetFile(tempParquetFile);
                        assertEquals(2, records.size());

                        // Verify first record
                        GenericRecord record1 = records.get(0);
                        assertEquals(1L, record1.get("sensorId"));
                        assertEquals("{\"value\": 100, \"status\": \"OK\"}", record1.get("jsonData").toString());

                        // Verify second record
                        GenericRecord record2 = records.get(1);
                        assertEquals(2L, record2.get("sensorId"));
                        assertEquals("{\"value\": 200, \"status\": \"WARN\"}", record2.get("jsonData").toString());

                    } catch (IOException e) {
                        fail("Test failed due to IOException during Parquet verification", e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void convertToParquet_withEmptyFlux_shouldProduceEmptyBytes() {
        // Arrange
        Flux<SensorData> emptyFlux = Flux.empty();

        // Act & Assert
        StepVerifier.create(parquetConversionService.convertToParquet(emptyFlux))
                .assertNext(parquetBytes -> {
                    assertNotNull(parquetBytes);
                    assertEquals(0, parquetBytes.length);
                })
                .verifyComplete();
    }

    // Helper method to read a Parquet file and return its records
    private List<GenericRecord> readParquetFile(File file) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        Path path = new Path(file.toURI());
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(path, new Configuration())).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }
}
