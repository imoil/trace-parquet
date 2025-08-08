package com.samsung.ees.infra.api.dataprovider.service;

import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.util.GzipUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * Service responsible for converting a stream of SensorData into a Parquet file format.
 * Refactored to load Avro schema from an external file.
 */
@Service
@Slf4j
public class ParquetConversionService {

    private static final Schema SCHEMA;

    // Load the Avro schema from the classpath resource file.
    static {
        try (InputStream schemaStream = ParquetConversionService.class.getResourceAsStream("/avro/ParameterRecord.avsc")) {
            if (schemaStream == null) {
                throw new IOException("Cannot find Avro schema file: ParameterRecord.avsc");
            }
            SCHEMA = new Schema.Parser().parse(schemaStream);
            log.info("Successfully loaded Avro schema for ParameterRecord.");
        } catch (IOException e) {
            log.error("Failed to load Avro schema.", e);
            throw new UncheckedIOException("Failed to initialize ParquetConversionService due to schema loading error.", e);
        }
    }

    /**
     * Converts a Flux of ParameterData into a Parquet file as a byte array.
     * ⚠️ This implementation uses collectList(), which buffers all data in memory.
     * For extremely large datasets, this may cause an OutOfMemoryError.
     * A true streaming implementation would be more complex and might require writing to a temporary file.
     *
     * @param sensorDataFlux The reactive stream of data to convert.
     * @return A Mono emitting the Parquet file as a byte array.
     */
    public Mono<byte[]> convertToParquet(Flux<ParameterData> sensorDataFlux) {
        return sensorDataFlux.collectList().flatMap(dataList -> {
            if (dataList.isEmpty()) {
                log.debug("Input data stream is empty. Returning empty byte array.");
                return Mono.just(new byte[0]);
            }

            log.info("Starting Parquet conversion for {} records.", dataList.size());
            return Mono.fromCallable(() -> {
                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                            try (ParquetWriter<GenericRecord> writer = createParquetWriter(baos)) {
                                for (ParameterData data : dataList) {
                                    writer.write(transformSensorData(data));
                                }
                            }
                            log.info("In-memory Parquet conversion completed successfully.");
                            return baos.toByteArray();
                        } catch (IOException e) {
                            log.error("Error during in-memory Parquet conversion", e);
                            throw new UncheckedIOException(e);
                        }
                    })
                    .subscribeOn(Schedulers.boundedElastic()) // CPU-intensive work on a dedicated thread pool
                    .onErrorMap(e -> new RuntimeException("Failed to convert data to Parquet", e));
        });
    }

    private ParquetWriter<GenericRecord> createParquetWriter(OutputStream outputStream) throws IOException {
        Configuration conf = new Configuration();
        // Disable CRC checks in Hadoop client for local file system operations, can prevent some warnings.
        conf.set("fs.file.impl.disable.cache", "true");
        return AvroParquetWriter.<GenericRecord>builder(new InMemoryOutputFile(outputStream))
                .withSchema(SCHEMA)
                .withConf(conf)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    private GenericRecord transformSensorData(ParameterData data) {
        try {
            String decompressedJson = GzipUtil.gzipDecompString(data.getTraceData());

            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("paramIndex", data.getParamIndex());
            record.put("startTime", java.sql.Timestamp.valueOf(data.getStartTime()).getTime());
            record.put("endTime", java.sql.Timestamp.valueOf(data.getEndTime()).getTime());
            record.put("traceData", decompressedJson);

            return record;
        } catch (IOException e) {
            log.error("Failed to decompress or process data for paramIndex {}: {}", data.getParamIndex(), e.getMessage());
            throw new UncheckedIOException("Data transformation failed for paramIndex " + data.getParamIndex(), e);
        }
    }

    // Helper class to allow ParquetWriter to write directly to an OutputStream.
    private static class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream baos;

        public InMemoryOutputFile(OutputStream os) {
            this.baos = (ByteArrayOutputStream) os;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new InMemoryPositionOutputStream(baos);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            baos.reset();
            return new InMemoryPositionOutputStream(baos);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static class InMemoryPositionOutputStream extends DelegatingPositionOutputStream {
        private final ByteArrayOutputStream baos;

        public InMemoryPositionOutputStream(ByteArrayOutputStream baos) {
            super(baos);
            this.baos = baos;
        }

        @Override
        public long getPos() {
            return baos.size();
        }
    }
}
