package com.samsung.ees.infra.api.dataprovider.service;

import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.util.GzipUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Service responsible for converting a stream of SensorData into a Parquet file format.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetConversionService {
    private static final String AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "ParameterRecord",
              "namespace": "com.samsung.ees.infra.api.dataprovider",
              "fields": [
                {"name": "paramIndex", "type": "long"},
                {"name": "startTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "endTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "traceData", "type": "string"}
              ]
            }
            """;
    private static final Schema SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA);

    public Mono<byte[]> convertToParquet(Flux<ParameterData> sensorDataFlux) {
        return sensorDataFlux.hasElements().flatMap(hasElements -> {
            if (Boolean.FALSE.equals(hasElements)) {
                log.debug("Input data stream is empty. Returning empty byte array.");
                return Mono.just(new byte[0]);
            }

            return Mono.usingWhen(
                            Mono.fromCallable(() -> Files.createTempFile("parquet-export-", ".parquet"))
                                    .subscribeOn(Schedulers.boundedElastic()),
                            tempFile -> Flux.usingWhen(
                                    Mono.fromCallable(() -> createParquetWriter(tempFile)),
                                    writer -> sensorDataFlux
                                            .publishOn(Schedulers.boundedElastic())
                                            .map(this::transformSensorData)
                                            .doOnNext(record -> {
                                                try {
                                                    writer.write(record);
                                                } catch (IOException e) {
                                                    throw new UncheckedIOException(e);
                                                }
                                            }),
                                    writer -> Mono.fromRunnable(() -> {
                                        try {
                                            writer.close();
                                        } catch (IOException e) {
                                            throw new UncheckedIOException(e);
                                        }
                                    }).subscribeOn(Schedulers.boundedElastic())
                            ).then(Mono.fromCallable(() -> Files.readAllBytes(tempFile))
                                    .subscribeOn(Schedulers.boundedElastic())),
                            tempFile -> Mono.fromRunnable(() -> {
                                try {
                                    Files.deleteIfExists(tempFile);
                                } catch (IOException e) {
                                    log.error("Failed to delete temporary file: {}", tempFile, e);
                                }
                            }).subscribeOn(Schedulers.boundedElastic())
                    )
                    .onErrorResume(e -> {
                        log.error("Error during Parquet conversion process", e);
                        return Mono.error(new RuntimeException("Failed to convert data to Parquet", e));
                    });
        });
    }

    private ParquetWriter<GenericRecord> createParquetWriter(Path tempFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl.disable.cache", "true");
        conf.setBoolean("dfs.client.use.datanode.hostname", false);

        return AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(tempFile.toUri()), conf))
                .withSchema(SCHEMA)
                .withConf(conf)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    /**
     * Transforms a SensorData object into a GenericRecord for Parquet writing.
     */
    private GenericRecord transformSensorData(ParameterData data) {
        try {
            // getBlobData() -> getTraceData()
            String decompressedJson = GzipUtil.gzipDecompString(data.getTraceData());

            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("paramIndex", data.getParamIndex());
            record.put("startTime", java.sql.Timestamp.valueOf(data.getStartTime()).getTime());
            record.put("endTime", java.sql.Timestamp.valueOf(data.getEndTime()).getTime());
            record.put("traceData", decompressedJson);

            return record;
        } catch (IOException e) {
            log.error("Failed to decompress or process data for paramIndex {}: {}", data.getParamIndex(), e.getMessage());
            throw new UncheckedIOException("Data transformation failed", e);
        }
    }
}