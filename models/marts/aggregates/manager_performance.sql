{{ config(
    materialized='view'
) }}

WITH manager_stats AS (
    SELECT
        m.manager_id,
        m.manager_name,
        t.team_name,
        tmr.effective_date,
        t.league_name,

        -- Total Games
        COUNT(fm.match_id) AS total_games_played,

        -- Total Wins
        SUM(CASE 
            WHEN (fm.home_team_id = t.team_id AND fm.outcome = 'Home') 
              OR (fm.away_team_id = t.team_id AND fm.outcome = 'Away') THEN 1 
            ELSE 0 
        END) AS total_games_won,

        -- Total Draws
        SUM(CASE WHEN fm.outcome = 'Draw' THEN 1 ELSE 0 END) AS total_games_drawn,

        -- Total Losses
        SUM(CASE 
            WHEN (fm.home_team_id = t.team_id AND fm.outcome = 'Away') 
              OR (fm.away_team_id = t.team_id AND fm.outcome = 'Home') THEN 1 
            ELSE 0 
        END) AS total_games_lost,

        -- Home & Away Games
        SUM(CASE WHEN fm.home_team_id = t.team_id THEN 1 ELSE 0 END) AS home_games_played,
        SUM(CASE WHEN fm.away_team_id = t.team_id THEN 1 ELSE 0 END) AS away_games_played,

        -- Home Wins, Draws, Losses
        SUM(CASE WHEN fm.home_team_id = t.team_id AND fm.outcome = 'Home' THEN 1 ELSE 0 END) AS home_games_won,
        SUM(CASE WHEN fm.home_team_id = t.team_id AND fm.outcome = 'Draw' THEN 1 ELSE 0 END) AS home_games_drawn,
        SUM(CASE WHEN fm.home_team_id = t.team_id AND fm.outcome = 'Away' THEN 1 ELSE 0 END) AS home_games_lost,

        -- Away Wins, Draws, Losses
        SUM(CASE WHEN fm.away_team_id = t.team_id AND fm.outcome = 'Away' THEN 1 ELSE 0 END) AS away_games_won,
        SUM(CASE WHEN fm.away_team_id = t.team_id AND fm.outcome = 'Draw' THEN 1 ELSE 0 END) AS away_games_drawn,
        SUM(CASE WHEN fm.away_team_id = t.team_id AND fm.outcome = 'Home' THEN 1 ELSE 0 END) AS away_games_lost,

        -- Goals Scored & Conceded
        SUM(CASE WHEN fm.home_team_id = t.team_id THEN fm.home_team_score ELSE fm.away_team_score END) AS total_goals_scored,
        SUM(CASE WHEN fm.home_team_id = t.team_id THEN fm.away_team_score ELSE fm.home_team_score END) AS total_goals_conceded

    FROM {{ ref('team_manager_relationships') }} tmr
    JOIN {{ ref('dim_managers') }} m ON tmr.manager_id = m.manager_id
    JOIN {{ ref('dim_teams') }} t ON tmr.team_id = t.team_id
    JOIN {{ ref('fact_match') }} fm 
        ON (fm.home_team_id = t.team_id OR fm.away_team_id = t.team_id)
        AND fm.played_on BETWEEN tmr.effective_date AND tmr.end_date
    GROUP BY 
        m.manager_id, m.manager_name, t.team_name, t.league_name, tmr.effective_date
),

percentage_stats AS (
    SELECT
        manager_id,
        manager_name,
        team_name,
        league_name,
        effective_date,
        total_games_played,
        total_games_won,
        total_games_drawn,
        total_games_lost,

        -- Win, Draw, Loss Percentages
        ROUND((total_games_won * 100.0) / NULLIF(total_games_played, 0), 2) AS pct_total_games_won,
        ROUND((total_games_drawn * 100.0) / NULLIF(total_games_played, 0), 2) AS pct_total_games_drawn,
        ROUND((total_games_lost * 100.0) / NULLIF(total_games_played, 0), 2) AS pct_total_games_lost,

        -- Home Win, Draw, Loss Percentages
        home_games_played,
        home_games_won,
        home_games_drawn,
        home_games_lost,
        ROUND((home_games_won * 100.0) / NULLIF(home_games_played, 0), 2) AS pct_home_games_won,
        ROUND((home_games_drawn * 100.0) / NULLIF(home_games_played, 0), 2) AS pct_home_games_drawn,
        ROUND((home_games_lost * 100.0) / NULLIF(home_games_played, 0), 2) AS pct_home_games_lost,

        -- Away Win, Draw, Loss Percentages
        away_games_played,
        away_games_won,
        away_games_drawn,
        away_games_lost,
        ROUND((away_games_won * 100.0) / NULLIF(away_games_played, 0), 2) AS pct_away_games_won,
        ROUND((away_games_drawn * 100.0) / NULLIF(away_games_played, 0), 2) AS pct_away_games_drawn,
        ROUND((away_games_lost * 100.0) / NULLIF(away_games_played, 0), 2) AS pct_away_games_lost,

        -- Goals Stats
        ROUND(total_goals_scored * 1.0 / NULLIF(total_games_played, 0), 2) AS avg_goals_scored_per_game,
        ROUND(total_goals_conceded * 1.0 / NULLIF(total_games_played, 0), 2) AS avg_goals_conceded_per_game

    FROM manager_stats
)

SELECT * FROM percentage_stats
