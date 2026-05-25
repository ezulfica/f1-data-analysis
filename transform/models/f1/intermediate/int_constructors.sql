WITH constructors AS (
    SELECT DISTINCT
        constructor_id,
        constructor_url,
        constructor_name,
        constructor_nationality
    FROM {{ ref("stg_results")}}
), 

new_constructors AS (
    SELECT 
        COALESCE(new_construct.new_id, constructors.constructor_id) AS constructor_id
    FROM constructors
    LEFT JOIN {{ref("constructor_newgroup")}} AS new_construct
    ON new_construct.constructor_id = constructors.constructor_id    
)

SELECT 
    DISTINCT new_constructors.constructor_id, 
    constructors.* EXCEPT(constructor_id)
FROM new_constructors
LEFT JOIN constructors
ON new_constructors.constructor_id = constructors.constructor_id