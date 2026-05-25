WITH driver_last_season AS (
    SELECT 
        driver_id, 
        MAX(season) as season
    FROM {{ref('stg_results')}}
    GROUP BY driver_id
)


SELECT
    results.*
FROM {{ref('stg_results')}} as results
INNER JOIN driver_last_season 
ON driver_last_season.driver_id = results.driver_id
AND driver_last_season.season = results.season


