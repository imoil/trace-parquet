package com.samsung.ees.infra.api.dataprovider.controller;

import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.repository.ParameterDataRepository;
import com.samsung.ees.infra.api.dataprovider.service.ParquetConversionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
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

    @MockBean
    private ParameterDataRepository parameterDataRepository;

    @MockBean
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

    // 💡 개선 사항: 잘못된 시간 범위에 대한 테스트 케이스 추가
    @Test
    void exportToParquet_withInvalidDateRange_shouldReturnBadRequest() {
        // Arrange
        String startTime = "2023-01-31T23:59:59";
        String endTime = "2023-01-01T00:00:00"; // startTime > endTime

        // Act & Assert
        webTestClient.get()
                .uri("/api/data/parameters/trace/parquet?parameterIndices=1,2&startTime={startTime}&endTime={endTime}", startTime, endTime)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void exportToParquet_withEmptyIndices_shouldReturnBadRequest() {
        webTestClient.get()
                .uri("/api/data/parameters/trace/parquet?parameterIndices=&startTime=2023-01-01T00:00:00&endTime=2023-01-31T23:59:59")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void exportToParquet_whenNoDataFound_shouldReturnNotFound() {
        // Arrange
        when(parameterDataRepository.findByIdsAndTimeRange(anyList(), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(Flux.empty());
        when(parquetConversionService.convertToParquet(any()))
                .thenReturn(Mono.just(new byte[0]));

        // Act & Assert
        webTestClient.get()
                .uri("/api/data/parameters/trace/parquet?parameterIndices=999&startTime=2023-01-01T00:00:00&endTime=2023-01-31T23:59:59")
                .exchange()
                .expectStatus().isNotFound();
    }
}
