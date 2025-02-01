{{ config(
    materialized='view'
) }}

WITH team_match_union AS (
    SELECT  
        mh.match_id,
        mh.home_team_id AS team_id,
        mh.home_team_score AS team_score,
        mh.away_team_score AS opponent_score,
        mh.home_team_possession AS team_possession,
        'HOME' AS match_role,
        mh.played_on,
        mh.outcome
    FROM {{ ref('fact_match') }} mh
    WHERE mh.was_game_postponed = FALSE

    UNION ALL

    SELECT  
        ma.match_id,
        ma.away_team_id AS team_id,
        ma.away_team_score AS team_score,
        ma.home_team_score AS opponent_score,
        ma.away_team_possession AS team_possession,
        'AWAY' AS match_role,
        ma.played_on,
        ma.outcome
    FROM {{ ref('fact_match') }} ma
    WHERE ma.was_game_postponed = FALSE
)

SELECT 
    tm.team_id, 
    dt.team_name, 
    dt.league_name, 
    dt.country_name, 
    dt.short_name,
    COUNT(*) AS total_games,

    -- Total Wins, Losses, Draws
    SUM(CASE WHEN (UPPER(outcome) = 'HOME' AND match_role = 'HOME') 
              OR (UPPER(outcome) = 'AWAY' AND match_role = 'AWAY') THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (UPPER(outcome) = 'AWAY' AND match_role = 'HOME') 
              OR (UPPER(outcome) = 'HOME' AND match_role = 'AWAY') THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN UPPER(outcome) = 'DRAW' THEN 1 ELSE 0 END) AS draws,

    -- Home Data Points
    SUM(CASE WHEN match_role = 'HOME' THEN 1 ELSE 0 END) AS played_at_home,
    SUM(CASE WHEN UPPER(outcome) = 'HOME' AND match_role = 'HOME' THEN 1 ELSE 0 END) AS won_at_home,
    SUM(CASE WHEN UPPER(outcome) = 'AWAY' AND match_role = 'HOME' THEN 1 ELSE 0 END) AS lost_at_home,
    SUM(CASE WHEN UPPER(outcome) = 'DRAW' AND match_role = 'HOME' THEN 1 ELSE 0 END) AS drawn_games_at_home,
    ROUND(AVG(CASE WHEN match_role = 'HOME' THEN team_possession ELSE NULL END),2) AS average_home_possession,
    ROUND(AVG(CASE WHEN match_role = 'HOME' THEN team_score ELSE NULL END),2) AS avg_home_goals_scored,
    ROUND(AVG(CASE WHEN match_role = 'HOME' THEN opponent_score ELSE NULL END),2) AS avg_home_goals_conceded,

    -- Away Data Points        
    SUM(CASE WHEN match_role = 'AWAY' THEN 1 ELSE 0 END) AS played_at_away,
    SUM(CASE WHEN UPPER(outcome) = 'AWAY' AND match_role = 'AWAY' THEN 1 ELSE 0 END) AS won_at_away,
    SUM(CASE WHEN UPPER(outcome) = 'HOME' AND match_role = 'AWAY' THEN 1 ELSE 0 END) AS lost_at_away,
    SUM(CASE WHEN UPPER(outcome) = 'DRAW' AND match_role = 'AWAY' THEN 1 ELSE 0 END) AS drawn_games_at_away,
    ROUND(AVG(CASE WHEN match_role = 'AWAY' THEN team_possession ELSE NULL END),2) AS average_away_possession,
    ROUND(AVG(CASE WHEN match_role = 'AWAY' THEN team_score ELSE NULL END),2) AS avg_away_goals_scored,
    ROUND(AVG(CASE WHEN match_role = 'AWAY' THEN opponent_score ELSE NULL END),2) AS avg_away_goals_conceded

FROM team_match_union tm
JOIN {{ ref('dim_teams') }} dt ON tm.team_id = dt.team_id
GROUP BY tm.team_id, dt.team_name, dt.league_name, dt.country_name, dt.short_name
