WITH gp_points AS (
    SELECT
        season, 
        driver_id,
        constructor_id,
        SUM(points_scored) as race_points,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) AS race_wins
    FROM 
        {{ref("stg_results")}}
    GROUP BY 
        season, driver_id, constructor_id
), 

sprint_points AS (
    SELECT
        season, 
        driver_id,
        SUM(points_scored) AS sprint_race_points,
        SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0 END) AS sprints_wins
    FROM 
        {{ref("stg_sprint")}}
    GROUP BY 
        season, driver_id
)

SELECT
    gp.season, 
    gp.driver_id,
    COALESCE(constructors.new_id, gp.constructor_id) AS constructor_id,
    IFNULL(gp.race_points,0) + IFNULL(sprint.sprint_race_points,0) AS drivers_points,
    SUM(IFNULL(gp.race_points, 0) + IFNULL(sprint.sprint_race_points, 0)) 
        OVER (PARTITION BY gp.season, gp.constructor_id) AS constructor_points,
    gp.race_wins, 
    sprint.sprints_wins

FROM
    gp_points AS gp
LEFT JOIN 
    sprint_points AS sprint
ON gp.season = sprint.season AND gp.driver_id = sprint.driver_id
LEFT JOIN {{ref("constructor_newgroup")}} AS constructors
ON constructors.constructor_id = gp.constructor_id
