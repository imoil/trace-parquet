package com.samsung.ees.infra.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Represents a single row from the 'AAA' table.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorData {
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long parameterIndex; // sensorId -> parameterIndex
    private byte[] traceData;    // blobData -> traceData
}