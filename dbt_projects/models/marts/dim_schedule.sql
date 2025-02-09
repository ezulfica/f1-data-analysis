SELECT DISTINCT
    season,
    round,
    Circuit_circuitId as circuit_id
FROM {{ source('f1_ingested', 'schedule') }}