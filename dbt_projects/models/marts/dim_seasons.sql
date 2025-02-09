SELECT DISTINCT
    season,
    round,
    date
    
FROM {{ {{ source('f1_ingested', 'schedule') }}}}
