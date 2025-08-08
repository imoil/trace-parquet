package com.samsung.ees.infra.api.dataprovider.repository;

import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
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
@Slf4j
@Repository
@RequiredArgsConstructor
public class ParameterDataRepository {
    private final DatabaseClient databaseClient;

    // Row 매핑 함수를 새로운 모델과 컬럼명에 맞게 수정합니다.
    public static final BiFunction<Row, RowMetadata, ParameterData> MAPPING_FUNCTION = (row, rowMetaData) -> new ParameterData(
            row.get("paramIndex", Long.class),
            row.get("startTime", LocalDateTime.class),
            row.get("endTime", LocalDateTime.class),
            byteBufferToBytes(row.get("traceData", ByteBuffer.class))
    );

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return new byte[0];
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    // 메서드 파라미터명을 sensorIds -> ids 로 변경
    public Flux<ParameterData> findByIdsAndTimeRange(List<Long> ids, LocalDateTime startTime, LocalDateTime endTime) {
        if (ids == null || ids.isEmpty()) {
            return Flux.empty();
        }
        String inClause = IntStream.range(0, ids.size())
                .mapToObj(i -> ":id_" + i)
                .collect(Collectors.joining(", "));
        String sql = String.format("""
                SELECT
                       dparam.PARAM_INDEX as paramIndex,
                       dparam.START_TIME as startTime,
                       dparam.END_TIME as endTime,
                       dparam.TRACE_DATA as traceData
                  FROM TD_FD_TRACE_PARAM dparam
                 WHERE dparam.PARAM_INDEX IN (%s)
                   AND START_TIME >= :startTime
                   AND START_TIME <= :endTime
                 ORDER BY PARAM_INDEX, START_TIME ASC
                """, inClause
        );
        log.debug("Executing SQL query with bindings: {}", sql);

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql);
        for (int i = 0; i < ids.size(); i++) {
            spec = spec.bind("id_" + i, ids.get(i));
        }
        spec = spec.bind("startTime", startTime);
        spec = spec.bind("endTime", endTime);

        return spec.map(MAPPING_FUNCTION).all();
    }
}