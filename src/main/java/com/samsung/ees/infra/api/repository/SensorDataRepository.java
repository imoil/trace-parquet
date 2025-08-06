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

/**
 * Repository for fetching sensor data from the Oracle database using R2DBC.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class SensorDataRepository {

    private final DatabaseClient databaseClient;

    // Row mapping function to convert a database row to a SensorData object
    public static final BiFunction<Row, RowMetadata, SensorData> MAPPING_FUNCTION = (row, rowMetaData) -> new SensorData(
            row.get("start_time", LocalDateTime.class),
            row.get("end_time", LocalDateTime.class),
            // Oracle returns numbers as BigDecimal, so handle potential conversion
            Long.valueOf(row.get("sensor_id", Number.class).longValue()),
            // The BLOB data is read into a ByteBuffer and then converted to a byte array
            byteBufferToBytes(row.get("blob_data", ByteBuffer.class))
    );

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return new byte[0];
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Fetches sensor data from the AAA table reactively based on sensor IDs and time range.
     * The query fetches data from the last 7 days.
     *
     * @param sensorIds A list of sensor IDs to filter by.
     * @return A Flux stream of SensorData objects.
     */
    public Flux<SensorData> findBySensorIdsAndTimeRange(List<Long> sensorIds) {
        if (sensorIds == null || sensorIds.isEmpty()) {
            return Flux.empty();
        }

        // Dynamically create the IN clause for the sensor IDs
        String inClause = String.join(",", sensorIds.stream().map(String::valueOf).toList());

        // The SQL query to be executed.
        // Note: Using SYSTIMESTAMP for Oracle. Adjust if your DB timezone setup differs.
        String sql = String.format(
                "SELECT start_time, end_time, sensor_id, blob_data " +
                "FROM AAA " +
                "WHERE sensor_id IN (%s) " +
                "AND start_time > SYSTIMESTAMP - INTERVAL '7' DAY " +
                "AND end_time < SYSTIMESTAMP",
                inClause
        );

        log.debug("Executing SQL query: {}", sql);

        return databaseClient.sql(sql)
                .map(MAPPING_FUNCTION)
                .all();
    }
}
