WITH match_data AS (
    SELECT 
        match_json.key::STRING AS MATCH_ID, 
        match_json.value AS match_value
    FROM {{ source('raw_match', 'json_load_raw') }},
    LATERAL FLATTEN(input => json_data) AS match_json
),

home_players AS (
    SELECT
        player.key::STRING AS PLAYER_NAME,
        m.MATCH_ID,
        m.match_value:home_team.name::STRING AS TEAM_NAME,
        TRY_CAST(player.value:ShirtNumber::STRING AS FLOAT) AS TEAM_NUMBER,
        TRY_CAST(player.value:StartedGame::STRING AS BOOLEAN) AS STARTED_GAME,
        TRY_CAST(player.value:WasSubstituted::STRING AS BOOLEAN) AS WAS_SUBSTITUTED,
        player.value:ReplacedBy::STRING AS REPLACED_BY,
        TRY_CAST(player.value:YellowCards::STRING AS STRING) AS YELLOW_CARDS,
        CASE 
            WHEN IS_ARRAY(player.value:YellowCardMinutes) 
            THEN ARRAY_TO_STRING(player.value:YellowCardMinutes, ',') 
            ELSE NULL 
        END AS YELLOW_CARD_MINUTES,
        TRY_CAST(player.value:RedCards::STRING AS FLOAT) AS RED_CARDS,
        CASE 
            WHEN IS_ARRAY(player.value:RedCardMinutes) 
            THEN ARRAY_TO_STRING(player.value:RedCardMinutes, ',') 
            ELSE NULL 
        END AS RED_CARD_MINUTES,
        CASE 
            WHEN IS_ARRAY(player.value:Goals) THEN ARRAY_SIZE(player.value:Goals)
            ELSE 0 
        END AS GOALS_COUNT,
        CASE 
            WHEN IS_ARRAY(player.value:Goals) 
            THEN ARRAY_TO_STRING(player.value:Goals, ',') 
            ELSE NULL 
        END AS GOALS_ARRAY,
        CASE 
            WHEN IS_ARRAY(player.value:Assists) THEN ARRAY_SIZE(player.value:Assists)
            ELSE 0 
        END AS ASSISTS_COUNT,
        CASE 
            WHEN IS_ARRAY(player.value:Assists) 
            THEN ARRAY_TO_STRING(player.value:Assists, ',') 
            ELSE NULL 
        END AS ASSISTS_ARRAY,
        TRY_CAST(player.value:MinutesPlayed::STRING AS FLOAT) AS MINUTES_PLAYED,
        'home' AS PLAYING_AS
    FROM match_data m,
    LATERAL FLATTEN(input => m.match_value:home_team.players) AS player
),

away_players AS (
    SELECT
        player.key::STRING AS PLAYER_NAME,
        m.MATCH_ID,
        m.match_value:away_team.name::STRING AS TEAM_NAME,
        TRY_CAST(player.value:ShirtNumber::STRING AS FLOAT) AS TEAM_NUMBER,
        TRY_CAST(player.value:StartedGame::STRING AS BOOLEAN) AS STARTED_GAME,
        TRY_CAST(player.value:WasSubstituted::STRING AS BOOLEAN) AS WAS_SUBSTITUTED,
        player.value:ReplacedBy::STRING AS REPLACED_BY,
        TRY_CAST(player.value:YellowCards::STRING AS STRING) AS YELLOW_CARDS,
        CASE 
            WHEN IS_ARRAY(player.value:YellowCardMinutes) 
            THEN ARRAY_TO_STRING(player.value:YellowCardMinutes, ',') 
            ELSE NULL 
        END AS YELLOW_CARD_MINUTES,
        TRY_CAST(player.value:RedCards::STRING AS FLOAT) AS RED_CARDS,
        CASE 
            WHEN IS_ARRAY(player.value:RedCardMinutes) 
            THEN ARRAY_TO_STRING(player.value:RedCardMinutes, ',') 
            ELSE NULL 
        END AS RED_CARD_MINUTES,
        CASE 
            WHEN IS_ARRAY(player.value:Goals) THEN ARRAY_SIZE(player.value:Goals)
            ELSE 0 
        END AS GOALS_COUNT,
        CASE 
            WHEN IS_ARRAY(player.value:Goals) 
            THEN ARRAY_TO_STRING(player.value:Goals, ',') 
            ELSE NULL 
        END AS GOALS_ARRAY,
        CASE 
            WHEN IS_ARRAY(player.value:Assists) THEN ARRAY_SIZE(player.value:Assists)
            ELSE 0 
        END AS ASSISTS_COUNT,
        CASE 
            WHEN IS_ARRAY(player.value:Assists) 
            THEN ARRAY_TO_STRING(player.value:Assists, ',') 
            ELSE NULL 
        END AS ASSISTS_ARRAY,
        TRY_CAST(player.value:MinutesPlayed::STRING AS FLOAT) AS MINUTES_PLAYED,
        'away' AS PLAYING_AS
    FROM match_data m,
    LATERAL FLATTEN(input => m.match_value:away_team.players) AS player
)

SELECT * FROM home_players
UNION ALL
SELECT * FROM away_players
