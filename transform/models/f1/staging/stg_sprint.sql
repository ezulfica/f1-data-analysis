SELECT 
    season,
    round,
    url AS race_url,
    raceName,
    Circuit_circuitId,
    Circuit_url AS circuit_url,
    Circuit_circuitName,
    Circuit_Location_lat,
    Circuit_Location_long,
    Circuit_Location_locality,
    Circuit_Location_country,
    date AS race_date,
    time AS race_time,
    
    SprintResults_number AS car_number,
    SprintResults_position AS final_position,
    SprintResults_positionText AS position_text,
    SprintResults_points AS points_scored,
    
    SprintResults_Driver_driverId AS driver_id,
    SprintResults_Driver_permanentNumber AS permanent_number,
    SprintResults_Driver_code AS driver_code,
    SprintResults_Driver_url AS driver_url,
    SprintResults_Driver_givenName AS driver_given_name,
    SprintResults_Driver_familyName AS driver_family_name,
    SprintResults_Driver_dateOfBirth AS driver_dob,
    SprintResults_Driver_nationality AS driver_nationality,
    
    SprintResults_Constructor_constructorId AS constructor_id,
    SprintResults_Constructor_url AS constructor_url,
    SprintResults_Constructor_name AS constructor_name,
    SprintResults_Constructor_nationality AS constructor_nationality,
    
    SprintResults_grid AS grid_position,
    SprintResults_laps AS laps_completed,
    SprintResults_status AS race_status,

    SprintResults_Time_millis AS total_time_millis,
    SprintResults_Time_time AS total_time,

    SprintResults_FastestLap_lap AS fastest_lap_number,
    SprintResults_FastestLap_Time_time AS fastest_lap_time

FROM {{source('f1_ingested', 'sprint')}}
