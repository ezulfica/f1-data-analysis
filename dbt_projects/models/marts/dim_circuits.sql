WITH source_circuit AS (
    SELECT DISTINCT
        raceName, 
        Circuit_circuitId as circuit_id,
        Circuit_circuitName as circuit_name,
        Circuit_Location_lat as circuit_lat,
        Circuit_Location_long as circuit_long,
        Circuit_Location_locality as circuit_locality,
        Circuit_Location_country as circuit_country
    FROM {{source("f1_ingested", "schedule")}}
)

SELECT 
    circuit.*, 
    countries.continent as continent
FROM source_circuit AS circuit
LEFT JOIN {{ref("countries")}} as countries
ON circuit.circuit_country = countries.country
WHERE circuit.circuit_country IS NOT NULL
