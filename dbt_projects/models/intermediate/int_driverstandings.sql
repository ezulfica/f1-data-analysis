SELECT 
    season,
    round AS total_race_count,
    CASE   
        WHEN points = 0 
        THEN MAX(position) OVER(PARTITION BY season) 
        ELSE position
        END AS driver_position,
    points AS driver_points,
    wins AS driver_wins,
    driver_id,
    constructor_id,
    CASE 
        WHEN position = 1 THEN 1
        ELSE 0
        END AS driver_champion
FROM {{ ref('stg_driverstandings') }} 