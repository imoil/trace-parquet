package com.samsung.ees.infra.api.service;

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
        parquetConversionService = new ParquetConversionService();
    }

    private byte[] createGzipData(String content) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(content.getBytes("UTF-8"));
        }
        return byteStream.toByteArray();
    }

    @Test
    void convertToParquet_shouldProduceValidParquetFile(@TempDir java.nio.file.Path tempDir) throws IOException {
        // 1. Arrange: 새로운 SensorData 모델에 맞게 테스트 데이터를 생성합니다.
        LocalDateTime now = LocalDateTime.now();
        SensorData data1 = new SensorData(now, now.plusHours(1), 1L, createGzipData("{\"value\": 100, \"status\": \"OK\"}"));
        SensorData data2 = new SensorData(now.plusMinutes(1), now.plusHours(1).plusMinutes(1), 2L, createGzipData("{\"value\": 200, \"status\": \"WARN\"}"));
        Flux<SensorData> sensorDataFlux = Flux.just(data1, data2);

        // 2. Act
        StepVerifier.create(parquetConversionService.convertToParquet(sensorDataFlux))
                .assertNext(parquetBytes -> {
                    // 3. Assert
                    assertNotNull(parquetBytes);
                    assertTrue(parquetBytes.length > 0);

                    File tempParquetFile;
                    try {
                        tempParquetFile = tempDir.resolve("test.parquet").toFile();
                        try (FileOutputStream fos = new FileOutputStream(tempParquetFile)) {
                            fos.write(parquetBytes);
                        }

                        List<GenericRecord> records = readParquetFile(tempParquetFile);
                        assertEquals(2, records.size());

                        // 검증: "sensorId" -> "parameterIndex" 로 필드명 확인
                        GenericRecord record1 = records.get(0);
                        assertEquals(1L, record1.get("parameterIndex"));
                        assertEquals("{\"value\": 100, \"status\": \"OK\"}", record1.get("jsonData").toString());

                        GenericRecord record2 = records.get(1);
                        assertEquals(2L, record2.get("parameterIndex"));
                        assertEquals("{\"value\": 200, \"status\": \"WARN\"}", record2.get("jsonData").toString());

                    } catch (IOException e) {
                        fail("Test failed due to IOException during Parquet verification", e);
                    }
                })
                .verifyComplete();
    }

    @Test
    void convertToParquet_withEmptyFlux_shouldProduceEmptyBytes() {
        Flux<SensorData> emptyFlux = Flux.empty();

        StepVerifier.create(parquetConversionService.convertToParquet(emptyFlux))
                .assertNext(parquetBytes -> {
                    assertNotNull(parquetBytes);
                    assertEquals(0, parquetBytes.length);
                })
                .verifyComplete();
    }

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