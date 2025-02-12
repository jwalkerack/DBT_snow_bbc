{{ config(materialized='table') }}

WITH player_match AS (
    SELECT PD.player_id ,MD.GAME_ID,TD.TEAM_ID,
    PS.TEAM_NUMBER1 AS  ShirtNumber,
    CAST(PS.YELLOW_CARDS AS INTEGER) AS YellowCards,
    CAST(PS.red_cards AS INTEGER) AS RedCards,
    CAST(PS.GOALS_COUNT AS INTEGER) AS Goals,
    CAST(PS.ASSISTS_COUNT AS INTEGER) AS Assists,
    CAST(PS.minutes_played AS INTEGER) AS MinutesPlayed,
    PS.PLAYER_STATUS AS Player_MatchStatus,
    CASE 
        WHEN Player_MatchStatus IN ('Played Subbed Off','Played Full Game') THEN 'Starter'
        WHEN Player_MatchStatus IN ('Played Subbed On','Played Subbed On and Subbed Off') THEN 'Sub'
        WHEN Player_MatchStatus IN ('Did Not Play') THEN 'Sqaud'
        ELSE 'Unknow'
    END AS player_matchRole

    FROM {{ ref('players') }} PS
    LEFT JOIN {{ ref('dim_players') }} PD ON PD.player_key = CONCAT(PS.player_name, ' - ', PS.team_name)
    Left Join {{ ref('dim_match') }} MD on  Md.bbc_id = PS.match_id
    Left Join {{ ref('dim_teams') }} TD on TD.TEAM_NAME = PS.TEAM_NAME
)

SELECT * FROM player_match


