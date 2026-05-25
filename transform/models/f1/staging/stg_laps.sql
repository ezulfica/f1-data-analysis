SELECT 
    season,
    round,
    url AS race_url,
    raceName,
    Circuit_circuitId,
    Circuit_url AS circuit_url,
    Circuit_circuitName,
    Circuit_Location_lat,
    Circuit_Location_long,
    Circuit_Location_locality,
    Circuit_Location_country,
    date AS race_date,
    time AS race_time,
    Laps_number,
    Laps_Timings_driverId AS driver_id,
    Laps_Timings_position AS position,
    Laps_Timings_time AS lap_time
FROM {{source('f1_ingested', 'laps')}}
