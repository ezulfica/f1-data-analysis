SELECT 
    season,
    round,
    ConstructorStandings_position AS position,
    ConstructorStandings_positionText AS position_text,
    ConstructorStandings_points AS points,
    ConstructorStandings_wins AS wins,
    ConstructorStandings_Constructor_constructorId AS constructor_id,
    ConstructorStandings_Constructor_url AS constructor_url,
    ConstructorStandings_Constructor_name AS constructor_name,
    ConstructorStandings_Constructor_nationality AS constructor_nationality
FROM {{ source('f1_ingested', 'constructorstandings') }}
