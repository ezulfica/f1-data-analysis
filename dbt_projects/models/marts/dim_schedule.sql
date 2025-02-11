SELECT DISTINCT
    CONCAT(season, "_", round, "_", Circuit_circuitId) as schedule_id, 
    CONCAT(season, "_", round) as season_round,
    season, 
    round,
    Circuit_circuitId as circuit_id,
    date
FROM {{ source('f1_ingested', 'schedule') }}