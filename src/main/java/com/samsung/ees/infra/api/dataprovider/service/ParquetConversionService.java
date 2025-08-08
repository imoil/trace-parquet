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
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

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

    // 💡 개선 사항: 임시 파일 대신 메모리 기반 스트림을 사용하여 디스크 I/O 제거 및 성능 향상
    public Mono<byte[]> convertToParquet(Flux<ParameterData> sensorDataFlux) {
        return sensorDataFlux.collectList().flatMap(dataList -> {
            if (dataList.isEmpty()) {
                log.debug("Input data stream is empty. Returning empty byte array.");
                return Mono.just(new byte[0]);
            }

            return Mono.fromCallable(() -> {
                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                            try (ParquetWriter<GenericRecord> writer = createParquetWriter(baos)) {
                                for (ParameterData data : dataList) {
                                    writer.write(transformSensorData(data));
                                }
                            }
                            return baos.toByteArray();
                        } catch (IOException e) {
                            log.error("Error during in-memory Parquet conversion", e);
                            throw new UncheckedIOException(e);
                        }
                    })
                    .subscribeOn(Schedulers.boundedElastic()) // CPU-intensive 작업을 별도 스레드에서 처리
                    .onErrorResume(e -> {
                        log.error("Error during Parquet conversion process", e);
                        return Mono.error(new RuntimeException("Failed to convert data to Parquet", e));
                    });
        });
    }

    private ParquetWriter<GenericRecord> createParquetWriter(OutputStream outputStream) throws IOException {
        Configuration conf = new Configuration();
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
            throw new UncheckedIOException("Data transformation failed", e);
        }
    }

    // ParquetWriter가 OutputStream에 직접 쓸 수 있도록 도와주는 헬퍼 클래스
    private static class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream baos;

        public InMemoryOutputFile(OutputStream os) {
            this.baos = (ByteArrayOutputStream) os;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return new InMemoryPositionOutputStream(baos);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
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
        public long getPos() throws IOException {
            return baos.size();
        }
    }
}
