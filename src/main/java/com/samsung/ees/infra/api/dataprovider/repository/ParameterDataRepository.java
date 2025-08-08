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

    /**
     * 💡 [수정] VARBINARY 타입을 byte[]로 직접 받도록 변경
     */
    public static final BiFunction<Row, RowMetadata, ParameterData> MAPPING_FUNCTION = (row, rowMetaData) -> {
        Number paramIndexNumber = row.get("paramIndex", Number.class);
        Long paramIndex = (paramIndexNumber != null) ? paramIndexNumber.longValue() : null;

        return new ParameterData(
                paramIndex,
                row.get("startTime", LocalDateTime.class),
                row.get("endTime", LocalDateTime.class),
                row.get("traceData", byte[].class) // ByteBuffer 대신 byte[]로 직접 가져옵니다.
        );
    };

    // 💡 [수정] 더 이상 필요 없는 byteBufferToBytes 메소드 제거
    /*
    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return new byte[0];
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
    */

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
