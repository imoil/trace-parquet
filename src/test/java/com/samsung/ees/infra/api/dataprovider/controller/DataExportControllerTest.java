package com.samsung.ees.infra.api.dataprovider.controller;

import com.samsung.ees.infra.api.dataprovider.exception.GlobalExceptionHandler;
import com.samsung.ees.infra.api.dataprovider.model.ParameterData;
import com.samsung.ees.infra.api.dataprovider.repository.ParameterDataRepository;
import com.samsung.ees.infra.api.dataprovider.service.ParquetConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/**
 * 💡 [개선] @WebFluxTest 대신 MockitoExtension을 사용한 단위 테스트로 변경.
 * Spring 컨텍스트를 로드하지 않아 테스트가 더 가볍고 빨라집니다.
 */
@ExtendWith(MockitoExtension.class)
class DataExportControllerTest {

    private WebTestClient webTestClient;

    @Mock // 💡 @MockBean 대신 Mockito의 @Mock 사용
    private ParameterDataRepository parameterDataRepository;

    @Mock // 💡 @MockBean 대신 Mockito의 @Mock 사용
    private ParquetConversionService parquetConversionService;

    @InjectMocks // 💡 @Mock으로 생성된 객체들을 컨트롤러에 주입
    private DataExportController dataExportController;

    @BeforeEach
    void setUp() {
        // 💡 WebTestClient를 컨트롤러에 직접 바인딩하고, 예외 핸들러를 수동으로 추가
        webTestClient = WebTestClient.bindToController(dataExportController)
                .controllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void exportToParquet_withValidParameters_shouldReturnOk() {
        // Arrange
        when(parameterDataRepository.findByIdsAndTimeRange(anyList(), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(Flux.just(new ParameterData(1L, LocalDateTime.now(), LocalDateTime.now(), new byte[0])));
        when(parquetConversionService.convertToParquet(any()))
                .thenReturn(Mono.just("dummy-parquet-data".getBytes()));

        URI uri = UriComponentsBuilder.fromPath("/api/data/parameters/trace/parquet")
                .queryParam("parameterIndices", "1,2")
                .queryParam("startTime", "2023-01-01T00:00:00")
                .queryParam("endTime", "2023-01-31T23:59:59")
                .build().toUri();

        // Act & Assert
        webTestClient.get().uri(uri)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType(MediaType.APPLICATION_OCTET_STREAM)
                .expectBody(byte[].class).isEqualTo("dummy-parquet-data".getBytes());
    }

    @Test
    void exportToParquet_withInvalidDate_shouldReturnBadRequest() {
        URI uri = UriComponentsBuilder.fromPath("/api/data/parameters/trace/parquet")
                .queryParam("parameterIndices", "1,2")
                .queryParam("startTime", "invalid-date")
                .queryParam("endTime", "2023-01-31T23:59:59")
                .build().toUri();

        webTestClient.get().uri(uri)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void exportToParquet_withMissingParameters_shouldReturnBadRequest() {
        URI uri = UriComponentsBuilder.fromPath("/api/data/parameters/trace/parquet")
                .queryParam("parameterIndices", "1,2")
                .queryParam("startTime", "2023-01-01T00:00:00")
                // endTime is missing
                .build().toUri();

        webTestClient.get().uri(uri)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("endTime cannot be null.");
    }

    @Test
    void exportToParquet_withInvalidDateRange_shouldReturnBadRequest() {
        URI uri = UriComponentsBuilder.fromPath("/api/data/parameters/trace/parquet")
                .queryParam("parameterIndices", "1,2")
                .queryParam("startTime", "2023-01-31T23:59:59")
                .queryParam("endTime", "2023-01-01T00:00:00") // startTime > endTime
                .build().toUri();

        webTestClient.get().uri(uri)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("Invalid date range: startTime cannot be after endTime.");
    }

    @Test
    void exportToParquet_withEmptyIndices_shouldReturnBadRequest() {
        URI uri = UriComponentsBuilder.fromPath("/api/data/parameters/trace/parquet")
                .queryParam("parameterIndices", "")
                .queryParam("startTime", "2023-01-01T00:00:00")
                .queryParam("endTime", "2023-01-31T23:59:59")
                .build().toUri();

        webTestClient.get().uri(uri)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("parameterIndices cannot be empty.");
    }

    @Test
    void exportToParquet_whenNoDataFound_shouldReturnNotFound() {
        // Arrange
        when(parameterDataRepository.findByIdsAndTimeRange(anyList(), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(Flux.empty());
        when(parquetConversionService.convertToParquet(any()))
                .thenReturn(Mono.just(new byte[0])); // Service returns empty bytes if flux is empty

        URI uri = UriComponentsBuilder.fromPath("/api/data/parameters/trace/parquet")
                .queryParam("parameterIndices", "999")
                .queryParam("startTime", "2023-01-01T00:00:00")
                .queryParam("endTime", "2023-01-31T23:59:59")
                .build().toUri();

        // Act & Assert
        webTestClient.get().uri(uri)
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.message").isEqualTo("No data found for the given criteria.");
    }
}
