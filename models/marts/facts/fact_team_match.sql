{{ config(materialized='table') }}

WITH team_match AS (
    SELECT  MD.GAME_ID ,DT.TEAM_ID,--MR.*
    MR.home_team_score AS scored,
    MR.Away_team_score AS conceeded, 
    MR.home_team_possession AS possession,
    REPLACE(MR.home_team_formation, 'Formation: ', '') AS formation,
    1 AS GameRole,
    CASE 
        WHEN home_team_score > away_team_score THEN 3  -- Win
        WHEN home_team_score < away_team_score THEN 0  -- Loss
        WHEN home_team_score = away_team_score THEN 1  -- Draw
        ELSE 0  -- Undefined case
    END AS Points,
    MR.played_on
    
    FROM {{ ref('match_results') }} MR
    Left Join {{ ref('dim_match') }} MD on MR.MATCH_ID  = MD.BBC_ID 
    Left Join {{ ref('dim_teams') }} DT on MR.HOME_TEAM_NAME  = DT.TEAM_NAME

    UNION

    SELECT  MD.GAME_ID ,DT.TEAM_ID,--MR.*
    MR.Away_team_score AS scored,
    MR.Home_team_score AS conceeded, 
    MR.Away_team_possession AS possession,
    REPLACE(MR.away_team_formation, 'Formation: ', '') AS formation,
    2 AS GameRole,
    CASE 
        WHEN home_team_score > away_team_score THEN 0  -- Loss
        WHEN home_team_score < away_team_score THEN 3  -- Win
        WHEN home_team_score = away_team_score THEN 1  -- Draw
        ELSE 0  -- Undefined case
    END AS Points,
    MR.played_on
    FROM {{ ref('match_results') }} MR
    Left Join {{ ref('dim_match') }} MD on MR.MATCH_ID  = MD.BBC_ID 
    Left Join {{ ref('dim_teams') }} DT on MR.away_TEAM_NAME  = DT.TEAM_NAME

)

SELECT
    ROW_NUMBER() OVER (ORDER BY played_on)  AS row_id,
    *,
    -- Total games (home + away)
    ROW_NUMBER() OVER (PARTITION BY TEAM_ID ORDER BY played_on ASC) AS game_number,

    -- Running total of home games
    CASE 
        WHEN GameRole = 1 THEN 
            SUM(1) OVER (PARTITION BY TEAM_ID, GameRole ORDER BY played_on ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ELSE NULL 
    END AS home_game_number,

    -- Running total of away games
    CASE 
        WHEN GameRole = 2 THEN 
            SUM(1) OVER (PARTITION BY TEAM_ID, GameRole ORDER BY played_on ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ELSE NULL 
    END AS away_game_number

FROM team_match
ORDER BY TEAM_ID, played_on

