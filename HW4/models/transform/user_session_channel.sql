WITH user_session_channel AS (
    SELECT
        userId,
        sessionId,
        channel
    FROM RAW.USER_SESSION_CHANNEL
)

SELECT *
FROM user_session_channel
