package com.samsung.ees.infra.api.dataprovider.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParameterData {
    private Long paramIndex;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private byte[] traceData;
}