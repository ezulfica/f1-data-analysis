SELECT DISTINCT
    driver_id, 
    permanent_number, 
    driver_code,
    driver_given_name,
    driver_family_name,
    driver_birthdate,
    driver_nationality
FROM {{ ref("stg_results")}}