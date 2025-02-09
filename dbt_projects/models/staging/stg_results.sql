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
    
    Results_number AS car_number,
    Results_position AS final_position,
    Results_positionText AS position_text,
    Results_points AS points_scored,
    
    Results_Driver_driverId AS driver_id,
    Results_Driver_permanentNumber AS permanent_number,
    Results_Driver_code AS driver_code,
    Results_Driver_url AS driver_url,
    Results_Driver_givenName AS driver_given_name,
    Results_Driver_familyName AS driver_family_name,
    Results_Driver_dateOfBirth AS driver_dob,
    Results_Driver_nationality AS driver_nationality,
    
    Results_Constructor_constructorId AS constructor_id,
    Results_Constructor_url AS constructor_url,
    Results_Constructor_name AS constructor_name,
    Results_Constructor_nationality AS constructor_nationality,
    
    Results_grid AS grid_position,
    Results_laps AS laps_completed,
    Results_status AS race_status,

    Results_Time_millis AS total_time_millis,
    Results_Time_time AS total_time,

    Results_FastestLap_rank AS fastest_lap_rank,
    Results_FastestLap_lap AS fastest_lap_number,
    Results_FastestLap_Time_time AS fastest_lap_time,
    Results_FastestLap_AverageSpeed_units AS fastest_lap_speed_units,
    Results_FastestLap_AverageSpeed_speed AS fastest_lap_speed

FROM {{source('f1_ingested', 'results')}}
