WITH session_timestamp AS (
    SELECT
        sessionId,
        ts
    FROM RAW.SESSION_TIMESTAMP
)

SELECT *
FROM session_timestamp
