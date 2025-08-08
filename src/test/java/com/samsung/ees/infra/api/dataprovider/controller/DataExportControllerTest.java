package com.samsung.ees.infra.api.dataprovider.controller;

import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.repository.ParameterDataRepository;
import com.samsung.ees.infra.api.dataprovider.service.ParquetConversionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.http.MediaType;
// 사용자분께서 찾아주신 정확한 경로로 수정합니다.
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@WebFluxTest(DataExportController.class)
class DataExportControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    // 올바른 @MockitoBean을 사용합니다.
    @MockitoBean
    private ParameterDataRepository parameterDataRepository;

    @MockitoBean
    private ParquetConversionService parquetConversionService;

    @Test
    void exportToParquet_withValidParameters_shouldReturnOk() {
        // Arrange
        when(parameterDataRepository.findByIdsAndTimeRange(anyList(), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(Flux.just(new ParameterData(1L, LocalDateTime.now(), LocalDateTime.now(), new byte[0])));
        when(parquetConversionService.convertToParquet(any()))
                .thenReturn(Mono.just("dummy-parquet-data".getBytes()));

        // Act & Assert
        webTestClient.get()
                .uri("/api/data/parameters/trace/parquet?parameterIndices=1,2&startTime=2023-01-01T00:00:00&endTime=2023-01-31T23:59:59")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_OCTET_STREAM)
                .expectBody(byte[].class).isEqualTo("dummy-parquet-data".getBytes());
    }

    @Test
    void exportToParquet_withInvalidDate_shouldReturnBadRequest() {
        webTestClient.get()
                .uri("/api/data/parameters/trace/parquet?parameterIndices=1,2&startTime=invalid-date&endTime=2023-01-31T23:59:59")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void exportToParquet_withMissingParameters_shouldReturnBadRequest() {
        webTestClient.get()
                .uri("/api/data/parameters/trace/parquet?parameterIndices=1,2&startTime=2023-01-01T00:00:00")
                .exchange()
                .expectStatus().isBadRequest();
    }
}