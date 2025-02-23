SELECT 
    *,
    RANK() OVER(PARTITION BY season ORDER BY drivers_points DESC) AS season_driver_rank, 
    RANK() OVER(PARTITION BY season ORDER BY constructor_points DESC) AS season_constructor_rank,
    CASE 
        WHEN drivers_points = MAX(drivers_points) OVER(PARTITION BY season) THEN 1
        ELSE 0
        END AS driver_champion,
    CASE 
        WHEN constructor_points = MAX(constructor_points) OVER(PARTITION BY season) THEN 1
        ELSE 0
        END AS constructor_champion
FROM {{ref("int_performances")}}
ORDER BY season DESC, season_driver_rank