WITH status_table AS (
    SELECT DISTINCT race_status AS status,
    FROM {{ ref('stg_results') }}
    )

SELECT 
    st.status AS status,
    cat.category AS category
FROM status_table AS st
LEFT JOIN {{ref('race_status_classification')}} AS cat
ON st.status = cat.status



