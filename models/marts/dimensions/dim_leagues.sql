{{ config(
    materialized='table'
) }}

WITH league_name AS (
    -- Extract distinct team and league names
    SELECT DISTINCT league_name FROM {{ ref('match_results') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY league_name desc ) AS league_id, -- Generate surrogate key
    league_name,
    SPLIT_PART(league_name, ' ', 1) AS country_name, -- First word of league name
    TRIM(SUBSTR(league_name, CHARINDEX(' ', league_name) + 1)) AS short_name -- Rest of the league name
FROM league_name