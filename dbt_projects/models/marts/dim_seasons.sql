SELECT DISTINCT season
FROM {{ source('f1_ingested', 'schedule') }}
