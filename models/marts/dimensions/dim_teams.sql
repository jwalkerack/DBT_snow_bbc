{{ config(
    materialized='table'
) }}

WITH unique_teams AS (
    -- Extract distinct team and league names
    SELECT DISTINCT home_team_name AS team_name, league_name FROM {{ ref('match_results') }}
    UNION
    SELECT DISTINCT away_team_name AS team_name, league_name FROM {{ ref('match_results') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY team_name) AS team_id, -- Generate surrogate key
    team_name,
    league_name,
    SPLIT_PART(league_name, ' ', 1) AS country_name, -- First word of league name
    TRIM(SUBSTR(league_name, CHARINDEX(' ', league_name) + 1)) AS short_name -- Rest of the league name
FROM unique_teams
