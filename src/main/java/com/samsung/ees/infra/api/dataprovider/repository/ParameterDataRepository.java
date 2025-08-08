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

/**
 * Repository for fetching sensor data from the Oracle database using R2DBC.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class ParameterDataRepository {
    private final DatabaseClient databaseClient;

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

    // 💡 개선 사항: 동적 IN 절 생성을 명명된 파라미터 바인딩으로 변경하여 코드 간결화
    public Flux<ParameterData> findByIdsAndTimeRange(List<Long> ids, LocalDateTime startTime, LocalDateTime endTime) {
        if (ids == null || ids.isEmpty()) {
            return Flux.empty();
        }

        String sql = """
            SELECT
                   dparam.PARAM_INDEX as paramIndex,
                   dparam.START_TIME as startTime,
                   dparam.END_TIME as endTime,
                   dparam.TRACE_DATA as traceData
              FROM TD_FD_TRACE_PARAM dparam
             WHERE dparam.PARAM_INDEX IN (:ids)
               AND dparam.START_TIME >= :startTime
               AND dparam.START_TIME <= :endTime
             ORDER BY dparam.PARAM_INDEX, dparam.START_TIME ASC
            """;
        log.debug("Executing SQL query: {}", sql);

        return databaseClient.sql(sql)
                .bind("ids", ids)
                .bind("startTime", startTime)
                .bind("endTime", endTime)
                .map(MAPPING_FUNCTION)
                .all();
    }
}
