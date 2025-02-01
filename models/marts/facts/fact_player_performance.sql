{{ config(
    materialized='table'
) }}

WITH base_performance AS (
    SELECT  
        ROW_NUMBER() OVER (ORDER BY m.match_id) AS player_performance_id, -- Surrogate key
        m.match_id, 
        p.player_id, 
        t.team_id,
        pl.minutes_played,
        pl.yellow_cards,
        pl.red_cards,
        pl.goals_count AS goals,
        pl.assists_count AS assists,
        pl.started_game,
        pl.was_substituted AS was_subbed
    FROM {{ ref('players') }} pl
    LEFT JOIN {{ ref('dim_teams') }} t 
       ON pl.team_name = t.team_name
    LEFT JOIN {{ ref('dim_players') }} p 
       ON CONCAT(pl.player_name, ' - ', pl.team_name) = p.player_key
    LEFT JOIN {{ ref('fact_match') }} m
       ON pl.match_id = m.bbcmatch_key
)

SELECT * FROM base_performance
