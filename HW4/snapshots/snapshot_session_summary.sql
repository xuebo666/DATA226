{% snapshot snapshot_session_summary %}

{{
    config(
        target_schema='RAW',
        unique_key='sessionId',
        strategy='timestamp',
	    updated_at='ts',
        invalidate_hard_deletes='True'
    )
}}

SELECT
    userId,
    sessionId,
    channel,
    ts
FROM {{ ref('session_summary') }}

{% endsnapshot %}
