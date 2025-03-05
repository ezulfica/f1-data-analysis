SELECT 
    season,
    round AS total_race_count,
    position AS constructor_position,
    points AS constructor_points,
    wins AS constructor_wins,
    constructor_id,
    CASE 
        WHEN position = 1 THEN 1
        ELSE 0
        END AS constructor_champion
FROM {{ ref('stg_constructorstandings') }}