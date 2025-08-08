package com.samsung.ees.infra.api.dataprovider.config;

import io.r2dbc.spi.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

/**
 * 💡 [수정] Database를 프로그래밍 방식으로 초기화합니다.
 * 16진수 변환 대신, 원본 JSON을 직접 GZIP으로 압축하여 데이터 손상 문제를 최종적으로 해결합니다.
 */
@Component
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final DatabaseClient databaseClient;

    public DataInitializer(ConnectionFactory connectionFactory) {
        this.databaseClient = DatabaseClient.create(connectionFactory);
    }

    @Override
    public void run(String... args) {
        log.info("Starting programmatic data initialization...");

        // 샘플 데이터 목록 (원본 JSON 문자열 사용)
        List<SampleData> sampleData = List.of(
                new SampleData(1L, "2024-01-10T10:00:00", "2024-01-10T10:00:05", "{\"value\": 100, \"status\": \"OK\"}"),
                new SampleData(2L, "2024-01-10T10:01:00", "2024-01-10T10:01:10", "{\"value\": 250, \"status\": \"WARN\", \"temp\": 45.5}"),
                new SampleData(3L, "2024-01-10T10:02:00", "2024-01-10T10:02:15", "{\"value\": 500, \"status\": \"CRITICAL\", \"pressure\": 1.5}")
        );

        // 데이터 삽입 전 테이블을 비우는 로직
        Mono<Void> deleteData = databaseClient.sql("DELETE FROM TD_FD_TRACE_PARAM").then();

        // 데이터를 순차적으로 삽입하는 로직
        Mono<Void> insertData = Flux.fromIterable(sampleData)
                .concatMap(this::insertSampleData)
                .then();

        // 삭제 후 삽입 실행
        deleteData
                .then(insertData)
                .doOnSubscribe(s -> log.info("Clearing existing data..."))
                .doOnSuccess(v -> log.info("Programmatic data initialization completed successfully."))
                .doOnError(e -> log.error("Error during programmatic data initialization", e))
                .subscribe();
    }

    private Mono<Void> insertSampleData(SampleData data) {
        String sql = "INSERT INTO TD_FD_TRACE_PARAM (PARAM_INDEX, START_TIME, END_TIME, TRACE_DATA) VALUES (:index, :start, :end, :data)";
        return databaseClient.sql(sql)
                .bind("index", data.paramIndex)
                .bind("start", data.startTime)
                .bind("end", data.endTime)
                .bind("data", data.getGzippedJson()) // 압축된 바이트 배열을 직접 바인딩
                .then();
    }

    // 원본 JSON 데이터를 GZIP 압축된 byte[]로 변환하는 헬퍼 클래스
    private record SampleData(Long paramIndex, LocalDateTime startTime, LocalDateTime endTime, String rawJson) {
        SampleData(Long paramIndex, String startTime, String endTime, String rawJson) {
            this(paramIndex, LocalDateTime.parse(startTime), LocalDateTime.parse(endTime), rawJson);
        }

        /**
         * 💡 [수정] 원본 JSON 문자열을 GZIP byte 배열로 압축합니다.
         */
        public byte[] getGzippedJson() {
            Objects.requireNonNull(rawJson, "Raw JSON string cannot be null");
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
                gzipStream.write(rawJson.getBytes(StandardCharsets.UTF_8));
                gzipStream.finish(); // 스트림을 완전히 종료하여 모든 데이터가 쓰이도록 보장
                return byteStream.toByteArray();
            } catch (IOException e) {
                // CommandLineRunner에서 발생하는 예외는 애플리케이션 시작을 중단시킬 수 있으므로 UncheckedIOException으로 래핑
                throw new UncheckedIOException("Failed to gzip content for paramIndex: " + this.paramIndex, e);
            }
        }
    }
}
