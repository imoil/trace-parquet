# Oracle DB to Parquet Export API (v0.0.2)

이 프로젝트는 Spring Boot 3.x 및 Project Reactor를 사용하여 Oracle 데이터베이스에서 대용량 센서 데이터를 리액티브 스트림으로 읽어온 후, GZIP으로 압축된 BLOB 데이터를 실시간으로 처리하여 Parquet 형식으로 변환하고 API를 통해 다운로드할 수 있도록 제공하는 서비스입니다.

---

## v0.0.2 주요 개선 사항

- **DTO(Data Transfer Object) 도입**: 요청 파라미터를 `DataExportRequest` DTO로 캡슐화하여 코드의 가독성과 유지보수성을 향상시켰습니다.
- **유효성 검사 강화**: `spring-boot-starter-validation`을 사용하여 요청 파라미터에 대한 유효성 검사(@NotEmpty, @NotNull 등)를 DTO 계층에서 수행합니다.
- **전역 예외 처리**: `@RestControllerAdvice`를 사용하여 API 전반의 예외(유효성 검사 실패, 데이터 없음, 서버 내부 오류 등)를 일관된 형식의 JSON 응답으로 처리하도록 개선했습니다.
- **Avro 스키마 외부화**: 하드코딩 되어있던 Avro 스키마를 외부 `.avsc` 파일(`src/main/resources/avro/ParameterRecord.avsc`)로 분리하여 스키마의 관리 용이성을 높였습니다.
- **테스트 방식 개선**: `@WebFluxTest`와 `@MockBean`을 사용하던 통합 테스트 방식에서, Mockito의 `@ExtendWith(MockitoExtension.class)`를 사용한 순수 단위 테스트 방식으로 변경하여 테스트 속도를 높이고 의존성을 줄였습니다.
- **메모리 사용량 경고**: `ParquetConversionService`가 현재 모든 데이터를 메모리에 로드 후 변환하는 방식(collectList)의 잠재적인 `OutOfMemoryError` 위험성을 코드 주석에 명시했습니다.

---

## 주요 기능

-   **Reactive Data Streaming**: Spring Data R2DBC를 사용하여 Oracle DB의 데이터를 Non-Blocking I/O 기반의 리액티브 스트림(`Flux`)으로 조회합니다.
-   **On-the-fly Decompression**: 스트리밍되는 각 데이터 행(Row)의 BLOB 컬럼에 포함된 GZIP 압축 데이터를 실시간으로 해제합니다.
-   **JSON to Parquet Conversion**: 압축 해제된 JSON 데이터를 Parquet 형식으로 변환합니다.
-   **REST API Endpoint**: 변환된 Parquet 파일 데이터를 `application/octet-stream` 형태로 응답하는 API 엔드포인트를 제공합니다.

## 시스템 요구사항

-   Java 21 or later
-   Maven 3.8 or later
-   Oracle Database 19c or later

## 설정 방법 (Oracle DB)

### 1. 데이터베이스 테이블 생성

Oracle 데이터베이스에 아래와 같은 구조의 테이블이 필요합니다.

```sql
CREATE TABLE TD_FD_TRACE_PARAM (
    PARAM_INDEX NUMBER,
    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,
    TRACE_DATA BLOB
);
```

### 2. 애플리케이션 설정

`src/main/resources/application.properties` 파일을 열고, 사용자의 Oracle 데이터베이스 환경에 맞게 R2DBC 연결 정보를 수정합니다.

```properties
# Oracle R2DBC Connection Settings
spring.r2dbc.url=r2dbc:oracle://<your-db-host>:<your-db-port>/<your-service-name>
spring.r2dbc.username=<your-username>
spring.r2dbc.password=<your-password>
```

## 빌드 및 실행

```bash
# 1. 프로젝트 빌드
mvn clean install

# 2. 애플리케이션 실행
java -jar target/trace-parquet-0.0.2.jar
```

## API 사용법

(API 명세는 아래 H2 섹션의 예시와 동일합니다.)

---

## 💡 로컬 개발 및 테스트 (H2 인메모리 DB)

Oracle DB 없이 로컬 환경에서 즉시 실행하고 테스트할 수 있도록 H2 인메모리 데이터베이스를 사용하는 방법입니다.

### 1. 설정

-   `pom.xml`에서 `oracle-r2dbc` 의존성을 주석 처리하고 `r2dbc-h2`, `h2` 의존성을 추가합니다.
-   `application.properties`에서 Oracle 연결 정보를 주석 처리하고 H2 설정을 활성화합니다.
-   `src/main/resources`에 `schema.sql` (테이블 생성)과 `data.sql` (샘플 데이터 삽입)을 추가합니다.

### 2. 애플리케이션 실행

위 설정이 완료된 상태에서 프로젝트를 빌드하고 실행합니다.

```bash
# 1. 프로젝트 빌드
mvn clean install

# 2. 애플리케이션 실행
# (pom.xml의 artifactId가 trace-parquet-0.0.3-h2.jar로 변경되었다고 가정)
java -jar target/trace-parquet-0.0.3-h2.jar
```

애플리케이션이 시작되면 H2 인메모리 DB가 자동으로 설정되고 `data.sql`의 샘플 데이터가 로드됩니다.

### 3. H2 웹 콘솔 접속

애플리케이션 실행 후, 웹 브라우저에서 아래 주소로 접속하여 DB를 확인할 수 있습니다.

-   **URL**: `http://localhost:8080/h2-console`
-   **JDBC URL**: `jdbc:h2:mem:testdb`
-   **User Name**: `sa`
-   **Password**: (비워두세요)

`Connect` 버튼을 누르면 `TD_FD_TRACE_PARAM` 테이블과 삽입된 샘플 데이터를 확인할 수 있습니다.

### 4. API 호출 테스트

`data.sql`에 삽입된 샘플 데이터(`PARAM_INDEX` 1, 2)를 조회하는 예시입니다.

```bash
curl --location 'http://localhost:8080/api/data/parameters/trace/parquet?parameterIndices=1,2&startTime=2024-01-01T00:00:00&endTime=2024-01-31T23:59:59' \
--output data.parquet \
--write-out "HTTP Status: %{http_code}\n"
```

요청이 성공하면 `HTTP Status: 200`이 출력되고, 현재 디렉토리에 `data.parquet` 파일이 생성됩니다.

### 5. Parquet File Open

Qstudio 를 실행하고, 하기 명령어로 파일을 로딩한다

```sql
CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('/home/imoil/repo/trace-parquet/data.parquet');

SELECT * FROM data LIMIT 111000;
```