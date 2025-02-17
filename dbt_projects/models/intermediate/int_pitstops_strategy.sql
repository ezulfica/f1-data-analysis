WITH pitstops_cte AS (
    SELECT 
        season, 
        round,
        driver_id, 
        MAX(stop) as pitstop_count, 
        MIN(pit_duration) as fastest_pitstop_duration, 
    FROM {{ ref("stg_pitstops")}}
    GROUP BY season, round, driver_id
)

SELECT 
    pitstops_cte.*, 
    
    CASE
        WHEN pitstop_count = 0 THEN "No pitstop" 
        WHEN pitstop_count = 1 THEN "1 pitstop"
        WHEN pitstop_count = 2 THEN "2 pitstops"
        ELSE "3+ pitstops"
        END AS pitstop_strategy,

    MIN(fastest_pitstop_duration) OVER (PARTITION BY season, round) as race_fastest_pitstop_duration,
    MIN(fastest_pitstop_duration) OVER (PARTITION BY season) as season_fastest_pitstop_duration

FROM pitstops_cte