package com.samsung.ees.infra.api.dataprovider.controller;

import com.samsung.ees.infra.api.dataprovider.dto.DataExportRequest;
import com.samsung.ees.infra.api.dataprovider.exception.NoDataFoundException;
import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.repository.ParameterDataRepository;
import com.samsung.ees.infra.api.dataprovider.service.ParquetConversionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * REST Controller for exporting sensor data as a Parquet file.
 * Refactored to use DTOs and global exception handling.
 */
@Slf4j
@RestController
@RequestMapping("/api/data/parameters/trace")
@RequiredArgsConstructor
public class DataExportController {
    private final ParameterDataRepository parameterDataRepository;
    private final ParquetConversionService parquetConversionService;

    @GetMapping("/parquet")
    public Mono<ResponseEntity<byte[]>> exportToParquet(@Valid DataExportRequest request) {

        log.info("Received request to export data for parameter indices: {} from {} to {}",
                request.getParameterIndices(), request.getStartTime(), request.getEndTime());

        if (request.getStartTime().isAfter(request.getEndTime())) {
            log.warn("Invalid date range: startTime {} is after endTime {}.", request.getStartTime(), request.getEndTime());
            // This can also be handled by a custom validator on the DTO.
            return Mono.error(new IllegalArgumentException("Invalid date range: startTime cannot be after endTime."));
        }

        Flux<ParameterData> sensorDataFlux = parameterDataRepository.findByIdsAndTimeRange(
                request.getParameterIndices(), request.getStartTime(), request.getEndTime());

        return parquetConversionService.convertToParquet(sensorDataFlux)
                .map(parquetBytes -> {
                    if (parquetBytes.length == 0) {
                        throw new NoDataFoundException("No data found for the given criteria.");
                    }

                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
                    headers.setContentDispositionFormData("attachment", "parameter_data.parquet");
                    headers.setContentLength(parquetBytes.length);

                    log.info("Successfully generated Parquet file of size: {} bytes", parquetBytes.length);
                    return new ResponseEntity<>(parquetBytes, headers, HttpStatus.OK);
                });
    }
}
