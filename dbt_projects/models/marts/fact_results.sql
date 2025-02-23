SELECT
    CONCAT(result.season, "_", result.round, "_", result.Circuit_circuitId) as schedule_id,
    result.season,
    result.round,
    result.driver_id,
    COALESCE(constructors.new_id, result.constructor_id) AS constructor_id, 
    sprint.race_status as sprint_status,
    sprint.final_position as sprint_final_position,
    sprint.points_scored as sprint_points,
    result.grid_position as race_grid_position,
    result.race_status,
    result.final_position as race_final_position, 
    result.points_scored as race_points,
    pitstops.* EXCEPT (season, round, driver_id)

FROM {{ ref('stg_results') }} as result
LEFT JOIN {{ ref('stg_sprint') }} as sprint
ON result.season = sprint.season
AND result.round = sprint.round
AND result.driver_id = sprint.driver_id

LEFT JOIN {{ref("constructor_newgroup")}} AS constructors
ON constructors.constructor_id = result.constructor_id

LEFT JOIN {{ ref('int_pitstops_strategy') }} as pitstops
ON result.season = pitstops.season
AND result.round = pitstops.round
AND result.driver_id = pitstops.driver_id
ORDER BY season DESC, round DESC, race_final_position

