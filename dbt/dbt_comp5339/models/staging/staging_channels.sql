WITH staging_channels AS (
    SELECT
      channel_id,
      channel_name,
      loaded_timestamp
    FROM {{ ref('src_channels') }}
)

SELECT
  channel_id AS channel_key,
  channel_id AS original_channel_id,
  channel_name
FROM staging_channels
