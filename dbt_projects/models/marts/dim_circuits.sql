SELECT 
    circuit.*, 
    countries.continent as continent
FROM {{ref("int_circuits")}} AS circuit
LEFT JOIN {{ref("countries")}} as countries
ON circuit.circuit_country = countries.country
WHERE circuit.circuit_country IS NOT NULL
ORDER BY circuit.circuit_id