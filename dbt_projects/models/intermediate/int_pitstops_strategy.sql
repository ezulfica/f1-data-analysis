SELECT 
    season, 
    round,
    driver_id, 
    MAX(stop) as pitstop_count, 
    MIN(pit_duration) as fastest_pitstop_duration
FROM {{ ref("stg_pitstops")}}
GROUP BY season, round, driver_id