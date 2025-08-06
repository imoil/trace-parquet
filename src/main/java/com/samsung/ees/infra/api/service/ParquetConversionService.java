package com.samsung.ees.infra.api.service;

import com.samsung.ees.infra.api.model.SensorData;
import com.samsung.ees.infra.api.util.GzipUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Service responsible for converting a stream of SensorData into a Parquet file format.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetConversionService {

    private final ObjectMapper objectMapper;

    // Avro schema definition for the Parquet file.
    private static final String AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "SensorRecord",
              "namespace": "com.samsung.ees.infra.api",
              "fields": [
                {"name": "startTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "endTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "sensorId", "type": "long"},
                {"name": "jsonData", "type": "string"}
              ]
            }
            """;
    private static final Schema SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA);

    /**
     * Converts a Flux of SensorData objects into a byte array representing a Parquet file.
     *
     * @param sensorDataFlux The reactive stream of sensor data.
     * @return A Mono emitting the Parquet file as a byte array. Returns an empty byte array if the input flux is empty.
     */
    public Mono<byte[]> convertToParquet(Flux<SensorData> sensorDataFlux) {
        // By using switchIfEmpty, we can provide a default value (empty byte array)
        // only when the source Flux is empty. This is more efficient than hasElements().
        return sensorDataFlux
                .collectList()
                .flatMap(list -> {
                    if (list.isEmpty()) {
                        // If the list of data is empty, return an empty byte array immediately.
                        log.debug("Input data stream is empty. Returning empty byte array.");
                        return Mono.just(new byte[0]);
                    }

                    // If there is data, proceed with Parquet file creation.
                    return Mono.fromCallable(() -> {
                        Path tempFile = Files.createTempFile("parquet-export-", ".parquet");
                        log.debug("Created temporary file for Parquet writing: {}", tempFile);

                        try (ParquetWriter<GenericRecord> writer = createParquetWriter(tempFile)) {
                            // Iterate over the collected list to write records.
                            for (SensorData data : list) {
                                writer.write(transformSensorData(data));
                            }
                        }

                        byte[] parquetBytes = Files.readAllBytes(tempFile);
                        Files.delete(tempFile);
                        log.debug("Successfully created Parquet data and deleted temporary file.");
                        return parquetBytes;

                    }).subscribeOn(Schedulers.boundedElastic());
                })
                .onErrorResume(e -> {
                    log.error("Error during Parquet conversion process", e);
                    return Mono.error(new RuntimeException("Failed to convert data to Parquet", e));
                });
    }

    /**
     * Creates a ParquetWriter for a given temporary file path.
     */
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
    private GenericRecord transformSensorData(SensorData data) {
        try {
            String decompressedJson = GzipUtil.decompress(data.getBlobData());

            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("startTime", java.sql.Timestamp.valueOf(data.getStartTime()).getTime());
            record.put("endTime", java.sql.Timestamp.valueOf(data.getEndTime()).getTime());
            record.put("sensorId", data.getSensorId());
            record.put("jsonData", decompressedJson);

            return record;
        } catch (IOException e) {
            log.error("Failed to decompress or process data for sensorId {}: {}", data.getSensorId(), e.getMessage());
            throw new RuntimeException("Data transformation failed", e);
        }
    }
}
