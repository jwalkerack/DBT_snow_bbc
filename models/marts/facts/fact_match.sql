{{ config(
    materialized='table'
) }}

WITH match_data AS (
    SELECT 
        match_id,
        played_on,
        home_team_name,
        home_team_score,
        home_team_possession,
        REPLACE(home_team_formation, 'Formation: ', '') AS home_team_formation,
        away_team_name,
        away_team_score,
        away_team_possession,
        REPLACE(away_team_formation, 'Formation: ', '') AS away_team_formation,
        venue AS venue_name,
        attendance,
        was_game_postponed,
        league_name
    FROM {{ ref('match_results') }}
),

fact_match AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY m.played_on) AS match_id,  -- Surrogate key
        m.match_id AS bbcmatch_key,
        m.played_on,

        -- Home Team Datapoints
        ht.team_id AS home_team_id,
        m.home_team_score,
        m.home_team_possession,
        m.home_team_formation,

        -- Away Team Datapoints
        at.team_id AS away_team_id,
        m.away_team_score,
        m.away_team_possession,
        m.away_team_formation,

        -- Venue Mapping
        v.venue_id AS venue_id,

        -- Derived Metrics
        m.home_team_score + m.away_team_score AS total_goals,
        m.attendance,
        m.was_game_postponed,

        -- Match Outcome
        CASE 
            WHEN m.home_team_score > m.away_team_score THEN 'Home'
            WHEN m.home_team_score < m.away_team_score THEN 'Away'
            ELSE 'Draw'
        END AS outcome,

        m.league_name
    FROM match_data m
    LEFT JOIN {{ ref('dim_teams') }} ht ON m.home_team_name = ht.team_name
    LEFT JOIN {{ ref('dim_teams') }} at ON m.away_team_name = at.team_name
    LEFT JOIN {{ ref('dim_venues') }} v ON m.venue_name = v.venue_name
)

SELECT * FROM fact_match
