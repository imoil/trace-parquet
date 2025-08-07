package com.samsung.ees.infra.api.repository;

import com.samsung.ees.infra.api.model.SensorData;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Repository for fetching sensor data from the Oracle database using R2DBC.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class SensorDataRepository {

    private final DatabaseClient databaseClient;

    // Row 매핑 함수를 새로운 모델과 컬럼명에 맞게 수정합니다.
    public static final BiFunction<Row, RowMetadata, SensorData> MAPPING_FUNCTION = (row, rowMetaData) -> new SensorData(
            row.get("start_time", LocalDateTime.class),
            row.get("end_time", LocalDateTime.class),
            Long.valueOf(row.get("parameter_index", Number.class).longValue()),
            byteBufferToBytes(row.get("trace_data", ByteBuffer.class))
    );

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return new byte[0];
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    // 메서드 파라미터명을 sensorIds -> parameterIndices 로 변경
    public Flux<SensorData> findBySensorIdsAndTimeRange(List<Long> parameterIndices, LocalDateTime startTime, LocalDateTime endTime) {
        if (parameterIndices == null || parameterIndices.isEmpty()) {
            return Flux.empty();
        }

        String inClause = IntStream.range(0, parameterIndices.size())
                .mapToObj(i -> ":id_" + i)
                .collect(Collectors.joining(", "));

        // SQL 쿼리의 컬럼명 변경 및 ORDER BY 절 추가
        String sql = String.format(
                "SELECT start_time, end_time, parameter_index, trace_data " +
                        "FROM AAA " +
                        "WHERE parameter_index IN (%s) " +
                        "AND start_time >= :startTime " +
                        "AND end_time <= :endTime " +
                        "ORDER BY parameter_index, start_time ASC", // 정렬 조건 추가
                inClause
        );

        log.debug("Executing SQL query with bindings: {}", sql);

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql);

        for (int i = 0; i < parameterIndices.size(); i++) {
            spec = spec.bind("id_" + i, parameterIndices.get(i));
        }

        spec = spec.bind("startTime", startTime);
        spec = spec.bind("endTime", endTime);

        return spec.map(MAPPING_FUNCTION).all();
    }
}