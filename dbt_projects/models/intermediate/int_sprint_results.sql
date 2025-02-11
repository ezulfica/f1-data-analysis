SELECT 
    season, 
    round,
    driver_id,
    race_status as sprint_status,
    points_scored as sprint_points, 
    final_position as sprint_final_position
FROM {{ ref('stg_sprint') }}

    



