SELECT DISTINCT race_status as status
FROM {{ ref('stg_results') }}

UNION DISTINCT

SELECT DISTINCT race_status as status
FROM {{ ref('stg_sprint') }}



