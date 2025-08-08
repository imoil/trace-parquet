package com.samsung.ees.infra.api.dataprovider.controller;

import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.repository.ParameterDataRepository;
import com.samsung.ees.infra.api.dataprovider.service.ParquetConversionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST Controller for exporting sensor data as a Parquet file.
 */
@Slf4j
@RestController
@RequestMapping("/api/data/parameters/trace")
@RequiredArgsConstructor
public class DataExportController {
    private final ParameterDataRepository parameterDataRepository;
    private final ParquetConversionService parquetConversionService;

    @GetMapping("/parquet")
    public Mono<ResponseEntity<byte[]>> exportToParquet(
            @RequestParam("parameterIndices") String parameterIndices,
            @RequestParam("startTime") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam("endTime") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime) {

        log.info("Received request to export data for parameter indices: {} from {} to {}", parameterIndices, startTime, endTime);

        // 💡 개선 사항: startTime이 endTime보다 늦는 경우에 대한 유효성 검사 추가
        if (startTime.isAfter(endTime)) {
            log.error("Invalid date range: startTime {} cannot be after endTime {}.", startTime, endTime);
            return Mono.just(ResponseEntity.badRequest().body("Invalid date range: startTime cannot be after endTime.".getBytes()));
        }

        List<Long> ids;
        try {
            ids = Arrays.stream(parameterIndices.split(","))
                    .map(String::trim)
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            log.error("Invalid parameterIndices parameter format: {}", parameterIndices, e);
            return Mono.just(ResponseEntity.badRequest().body("Invalid format for parameterIndices.".getBytes()));
        }

        if (ids.isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().body("parameterIndices cannot be empty.".getBytes()));
        }

        Flux<ParameterData> sensorDataFlux = parameterDataRepository.findByIdsAndTimeRange(ids, startTime, endTime);
        return parquetConversionService.convertToParquet(sensorDataFlux)
                .map(parquetBytes -> {
                    if (parquetBytes.length == 0) {
                        log.warn("No data found for the given criteria. Returning 404 Not Found.");
                        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(parquetBytes);
                    }

                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
                    headers.setContentDispositionFormData("attachment", "parameter_data.parquet");
                    headers.setContentLength(parquetBytes.length);

                    log.info("Successfully generated Parquet file of size: {} bytes", parquetBytes.length);
                    return new ResponseEntity<>(parquetBytes, headers, HttpStatus.OK);
                })
                .doOnError(e -> log.error("An error occurred during Parquet export", e))
                .onErrorResume(e -> Mono.just(
                        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An internal error occurred.".getBytes())
                ));
    }
}
