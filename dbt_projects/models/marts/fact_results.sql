SELECT
    result.season,
    result.round,
    result.driver_id,
    result.constructor_id, 
    sprint.sprint_status,
    sprint.sprint_final_position,
    sprint.sprint_points,
    result.race_grid_position,
    result.race_status,
    result.race_final_position, 
    result.race_points,
    pitstops.* EXCEPT (season, round, driver_id)

FROM {{ ref('int_results') }} as result
LEFT JOIN {{ ref('int_sprint_results') }} as sprint
ON result.season = sprint.season
AND result.round = sprint.round
AND result.driver_id = sprint.driver_id
LEFT JOIN {{ ref('int_pitstops_strategy') }} as pitstops
ON result.season = pitstops.season
AND result.round = pitstops.round
AND result.driver_id = pitstops.driver_id


