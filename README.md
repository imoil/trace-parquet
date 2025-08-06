# Oracle DB to Parquet Export API

이 프로젝트는 Spring Boot 3.x 및 Project Reactor를 사용하여 Oracle 데이터베이스에서 대용량 센서 데이터를 리액티브 스트림으로 읽어온 후, GZIP으로 압축된 BLOB 데이터를 실시간으로 처리하여 Parquet 형식으로 변환하고 API를 통해 다운로드할 수 있도록 제공하는 서비스입니다.

## 주요 기능

-   **Reactive Data Streaming**: Spring Data R2DBC를 사용하여 Oracle DB의 데이터를 Non-Blocking I/O 기반의 리액티브 스트림(`Flux`)으로 조회합니다.
-   **On-the-fly Decompression**: 스트리밍되는 각 데이터 행(Row)의 BLOB 컬럼에 포함된 GZIP 압축 데이터를 실시간으로 해제합니다.
-   **JSON to Parquet Conversion**: 압축 해제된 JSON 데이터를 Parquet 형식으로 변환합니다.
-   **REST API Endpoint**: 변환된 Parquet 파일 데이터를 `application/octet-stream` 형태로 응답하는 API 엔드포인트를 제공합니다.

## 시스템 요구사항

-   Java 21 or later
-   Maven 3.8 or later
-   Oracle Database 19c or later
-   (선택) Docker

## 설정 방법

### 1. 데이터베이스 테이블 생성

Oracle 데이터베이스에 아래와 같은 구조의 테이블이 필요합니다.

```sql
CREATE TABLE AAA (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    sensor_id NUMBER,
    blob_data BLOB
);
```

### 2. 애플리케이션 설정

`src/main/resources/application.properties` 파일을 열고, 사용자의 Oracle 데이터베이스 환경에 맞게 R2DBC 연결 정보를 수정합니다.

```properties
# Spring WebFlux Server Port
server.port=8080

# Oracle R2DBC Connection Settings
spring.r2dbc.url=r2dbc:oracle://<your-db-host>:<your-db-port>/<your-service-name>
spring.r2dbc.username=<your-username>
spring.r2dbc.password=<your-password>

# Logging
logging.level.org.springframework.r2dbc=DEBUG
logging.level.com.samsung.ees.infra.api=INFO
```

## 빌드 및 실행 (Maven 기준)

### 1. 프로젝트 빌드

프로젝트 루트 디렉토리에서 아래 명령어를 실행하여 애플리케이션을 빌드합니다.

```bash
mvn clean install
```

### 2. 테스트 실행

TDD 방식으로 개발되었으며, 아래 명령어로 모든 테스트를 실행할 수 있습니다.

```bash
mvn test
```

### 3. 애플리케이션 실행

아래 명령어를 사용하여 애플리케이션을 시작합니다.

```bash
mvn spring-boot:run
```

또는 `target` 디렉토리에 생성된 JAR 파일을 직접 실행할 수도 있습니다.

```bash
java -jar target/trace-parquet-0.0.1.jar
```

애플리케이션이 성공적으로 시작되면 8080 포트에서 실행됩니다.

## API 사용법

애플리케이션 실행 후, 아래 `curl` 명령어를 사용하여 API를 호출할 수 있습니다. `sensorIds` 파라미터로 조회할 센서 ID 목록을 전달합니다.

```bash
curl --location 'http://localhost:8080/api/export/parquet?sensorIds=1,2,3' \
--output data.parquet
```

요청이 성공하면, 현재 디렉토리에 `data.parquet` 파일이 생성됩니다. 이 파일은 Parquet 뷰어 또는 데이터 분석 도구(예: Pandas, Spark)를 통해 내용을 확인할 수 있습니다.
