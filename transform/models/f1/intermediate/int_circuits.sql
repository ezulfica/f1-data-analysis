WITH circuit_last_apparition AS (
    SELECT 
        Circuit_circuitId as circuit_id, 
        MAX(season) as season
    FROM {{source('f1_ingested', 'schedule')}}
    GROUP BY circuit_id
), 

source_circuit AS (
    SELECT
        circuits.raceName, 
        cla.circuit_id,
        circuits.Circuit_circuitName as circuit_name,
        circuits.Circuit_Location_lat as circuit_lat,
        circuits.Circuit_Location_long as circuit_long,
        circuits.Circuit_Location_locality as circuit_locality,
        circuits.Circuit_Location_country as circuit_country
    FROM {{source("f1_ingested", "schedule")}} as circuits
    INNER JOIN circuit_last_apparition as cla 
    ON circuits.Circuit_circuitId = cla.circuit_id
    AND circuits.season = cla.season
    ORDER BY cla.circuit_id
)

SELECT * 
FROM source_circuit
