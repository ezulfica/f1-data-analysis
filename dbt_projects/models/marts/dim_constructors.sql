SELECT DISTINCT
    constructor_id,
    constructor_url,
    constructor_name,
    constructor_nationality
FROM {{ ref("int_constructor")}}
ORDER BY constructor_id