{# SELECT 
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
FROM {{ref("int_performances")}} #}

SELECT 
    d.* EXCEPT(constructor_id), 
    c.constructor_position, 
    c.constructor_points,
    c.constructor_wins,
    c.constructor_champion, 
    COALESCE(constructors.new_id, d.constructor_id) AS constructor_id,
FROM {{ref('int_driverstandings')}} AS d
LEFT JOIN {{ref('int_constructorstandings')}} AS c
ON d.constructor_id = c.constructor_id
AND d.season = c.season
LEFT JOIN {{ref("constructor_newgroup")}} AS constructors
ON d.constructor_id = constructors.constructor_id