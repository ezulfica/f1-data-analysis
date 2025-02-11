SELECT 
    season, 
    round,
    driver_id,
    Circuit_circuitId, 
    constructor_id, 
    grid_position as race_grid_position,
    final_position as race_final_position, 
    points_scored as race_points,
    race_status
FROM {{ ref('stg_results')}} 
