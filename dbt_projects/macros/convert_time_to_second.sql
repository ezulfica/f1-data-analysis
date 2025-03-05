{% macro convert_time_to_seconds(time_str) %}
  CASE 
    -- Format: MM:ss.mmm
    WHEN ARRAY_LENGTH(SPLIT({{ time_str }}, ':')) = 2 
      AND ARRAY_LENGTH(SPLIT(SPLIT({{ time_str }}, ':')[SAFE_OFFSET(1)], '.')) = 2
    THEN 
      SAFE_CAST(SPLIT({{ time_str }}, ':')[SAFE_OFFSET(0)] AS FLOAT64) * 60
      + SAFE_CAST(SPLIT(SPLIT({{ time_str }}, ':')[SAFE_OFFSET(1)], '.')[SAFE_OFFSET(0)] AS FLOAT64)
      + SAFE_CAST(SPLIT(SPLIT({{ time_str }}, ':')[SAFE_OFFSET(1)], '.')[SAFE_OFFSET(1)] AS FLOAT64) / 1000

    -- Format: ss.mmm (no minutes)
    WHEN ARRAY_LENGTH(SPLIT({{ time_str }}, ':')) = 1 
      AND ARRAY_LENGTH(SPLIT({{ time_str }}, '.')) = 2
    THEN 
      SAFE_CAST(SPLIT({{ time_str }}, '.')[SAFE_OFFSET(0)] AS FLOAT64)
      + SAFE_CAST(SPLIT({{ time_str }}, '.')[SAFE_OFFSET(1)] AS FLOAT64) / 1000

    ELSE NULL
  END
{% endmacro %}
