package com.samsung.ees.infra.api.dataprovider.dto;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO for data export request parameters.
 * Includes validation rules for the request.
 */
@Data
public class DataExportRequest {
    @NotEmpty(message = "parameterIndices cannot be empty.")
    private List<Long> parameterIndices;

    @NotNull(message = "startTime cannot be null.")
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime startTime;

    @NotNull(message = "endTime cannot be null.")
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime endTime;
}
