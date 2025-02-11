SELECT DISTINCT
    constructor_id,
    constructor_url,
    constructor_name,
    constructor_nationality
FROM {{ ref("stg_results")}}