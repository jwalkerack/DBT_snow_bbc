{{ config(
    materialized='table'
) }}

WITH unique_players AS (
    SELECT DISTINCT 
        player_name,
        team_name,
        team_number
    FROM {{ ref('players') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY player_name, team_name) AS player_id, -- Surrogate key
    CONCAT(player_name, ' - ', team_name) AS player_key, -- Composite key
    player_name,
    team_name,
    team_number
FROM unique_players
