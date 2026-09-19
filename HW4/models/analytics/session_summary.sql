WITH user_session_channel AS (
    SELECT *
    FROM {{ ref('user_session_channel') }}
),

session_timestamp AS (
    SELECT *
    FROM {{ ref('session_timestamp') }}
)

SELECT
    usc.userId,
    usc.sessionId,
    usc.channel,
    st.ts
FROM user_session_channel usc
JOIN session_timestamp st
    ON usc.sessionId = st.sessionId
