package com.samsung.ees.infra.api.controller;

import com.samsung.ees.infra.api.model.SensorData;
import com.samsung.ees.infra.api.repository.SensorDataRepository;
import com.samsung.ees.infra.api.service.ParquetConversionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST Controller for exporting sensor data as a Parquet file.
 */
@RestController
@RequestMapping("/api/export")
@RequiredArgsConstructor
@Slf4j
public class DataExportController {

    private final SensorDataRepository sensorDataRepository;
    private final ParquetConversionService parquetConversionService;

    /**
     * API endpoint to fetch sensor data and return it as a Parquet file.
     *
     * @param sensorIds A comma-separated string of sensor IDs to query.
     * @return A ResponseEntity containing the Parquet file bytes.
     */
    @GetMapping("/parquet")
    public Mono<ResponseEntity<byte[]>> exportToParquet(@RequestParam("sensorIds") String sensorIds) {
        log.info("Received request to export data for sensor IDs: {}", sensorIds);

        List<Long> idList;
        try {
            idList = Arrays.stream(sensorIds.split(","))
                    .map(String::trim)
                    .map(Long::parseLong)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            log.error("Invalid sensorIds parameter format: {}", sensorIds, e);
            return Mono.just(ResponseEntity.badRequest().build());
        }

        if (idList.isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().build());
        }

        // 1. Fetch data from the repository as a Flux
        Flux<SensorData> sensorDataFlux = sensorDataRepository.findBySensorIdsAndTimeRange(idList);

        // 2. Convert the Flux to a Parquet byte array using the service
        return parquetConversionService.convertToParquet(sensorDataFlux)
                .map(parquetBytes -> {
                    if (parquetBytes.length == 0) {
                        // If no data was found or processed, return Not Found
                        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(parquetBytes);
                    }
                    
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
                    headers.setContentDispositionFormData("attachment", "sensor_data.parquet");
                    headers.setContentLength(parquetBytes.length);

                    log.info("Successfully generated Parquet file of size: {} bytes", parquetBytes.length);
                    return new ResponseEntity<>(parquetBytes, headers, HttpStatus.OK);
                })
                .doOnError(e -> log.error("An error occurred during Parquet export", e))
                .onErrorResume(e -> Mono.just(
                        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build()
                ));
    }
}
