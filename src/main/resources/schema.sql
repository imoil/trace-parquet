-- H2 데이터베이스에서 사용할 테이블 생성 스크립트

-- 💡 [참고] 원본 Oracle DB 테이블 구조
-- CREATE TABLE TD_FD_TRACE_PARAM (
--     PARAM_INDEX NUMBER,
--     START_TIME TIMESTAMP,
--     END_TIME TIMESTAMP,
--     TRACE_DATA BLOB
-- );

-- 💡 [수정] H2 호환성을 위해 데이터 타입을 VARBINARY로 변경
DROP TABLE IF EXISTS TD_FD_TRACE_PARAM;

CREATE TABLE TD_FD_TRACE_PARAM (
    PARAM_INDEX NUMBER NOT NULL PRIMARY KEY,
    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,
    TRACE_DATA VARBINARY(512)
);
