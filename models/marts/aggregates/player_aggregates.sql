{{ config(
    materialized='table'
) }}

WITH players_rolled AS (
    SELECT 
        p.player_id,
        p.minutes_played,
        p.yellow_cards,
        p.red_cards,
        p.goals,
        p.assists,
        p.started_game,
        p.was_subbed,
        'Home' AS playedAt,
        mh.home_team_id AS team_id,
        p.match_id
    FROM {{ ref('fact_player_performance') }} p
    RIGHT JOIN {{ ref('fact_match') }} mh
        ON mh.match_id = p.match_id AND p.team_id = mh.home_team_id

    UNION ALL

    SELECT 
        p.player_id,
        p.minutes_played,
        p.yellow_cards,
        p.red_cards,
        p.goals,
        p.assists,
        p.started_game,
        p.was_subbed,
        'Away' AS playedAt,
        ma.away_team_id AS team_id,
        p.match_id
    FROM {{ ref('fact_player_performance') }} p
    RIGHT JOIN {{ ref('fact_match') }} ma
        ON ma.match_id = p.match_id AND p.team_id = ma.away_team_id
),

players_that_did_get_on AS (
    SELECT * 
    FROM players_rolled 
    WHERE minutes_played > 0
),

player_aggregates AS (
    SELECT 
        player_id,
        team_id,
        COUNT(*) AS total_games_played,
        SUM(CASE WHEN started_game = TRUE THEN 1 ELSE 0 END) AS total_games_started,
        SUM(CASE WHEN started_game = TRUE AND was_subbed = TRUE THEN 1 ELSE 0 END) AS total_games_subbed_off,
        SUM(CASE WHEN started_game = FALSE AND minutes_played > 0 THEN 1 ELSE 0 END) AS total_games_subbed_on,
        SUM(minutes_played) AS total_minutes,
        SUM(CASE WHEN started_game = TRUE THEN minutes_played ELSE 0 END) AS total_minutes_as_starter,
        SUM(CASE WHEN started_game = FALSE THEN minutes_played ELSE 0 END) AS total_minutes_as_sub,
        ROUND(AVG(minutes_played), 2) AS avg_minutes,
        ROUND(AVG(CASE WHEN started_game = TRUE THEN minutes_played ELSE NULL END), 2) AS avg_minutes_as_started,
        ROUND(AVG(CASE WHEN started_game = FALSE THEN minutes_played ELSE NULL END), 2) AS avg_minutes_as_sub,
        SUM(goals) AS total_goals,
        SUM(assists) AS total_assists,
        SUM(yellow_cards) AS total_yellow_cards,
        SUM(red_cards) AS total_red_cards,
        ROUND(COALESCE(SUM(minutes_played) / NULLIF(SUM(goals), 0), 0), 2) AS minutes_per_goal
    FROM players_that_did_get_on
    GROUP BY player_id, team_id
),

home_away_agg AS (
    SELECT 
        player_id,
        SUM(CASE WHEN playedAt = 'Home' THEN 1 ELSE 0 END) AS total_games_home,
        SUM(CASE WHEN playedAt = 'Away' THEN 1 ELSE 0 END) AS total_games_away,
        SUM(CASE WHEN playedAt = 'Home' THEN minutes_played ELSE 0 END) AS total_minutes_home,
        SUM(CASE WHEN playedAt = 'Away' THEN minutes_played ELSE 0 END) AS total_minutes_away
    FROM players_that_did_get_on
    GROUP BY player_id
)

SELECT 
    p.*,
    ha.total_games_home,
    ha.total_games_away,
    ha.total_minutes_home,
    ha.total_minutes_away
FROM player_aggregates p
JOIN home_away_agg ha ON p.player_id = ha.player_id
ORDER BY p.total_games_played DESC
