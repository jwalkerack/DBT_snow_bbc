WITH match_data AS (
    SELECT 
        match_json.value:match_id::STRING AS match_id, 
        match_json.value AS match_value
    FROM {{ source('raw_match', 'football_data_json') }},
    LATERAL FLATTEN(input => GAMES_JSON) AS match_json
),

home_players AS (
    SELECT
        player.key::STRING AS PLAYER_NAME,
        m.MATCH_ID,

        m.match_value:home_team.name::STRING AS TEAM_NAME,
        player.value:ShirtNumber::STRING AS TEAM_NUMBER,
        CAST(REPLACE(player.value:ShirtNumber, ',', '') AS INT) AS TEAM_NUMBER1,
        TRY_CAST(player.value:WasStarter::STRING AS BOOLEAN) AS STARTED_GAME,
        TRY_CAST(player.value:WasSubstituted::STRING AS BOOLEAN) AS WAS_SUBSTITUTED,
        TRY_CAST(player.value:WasIntroduced::STRING AS BOOLEAN) AS Was_Introduced,
        TRY_CAST(player.value:is_captain::STRING AS BOOLEAN) AS is_captain,
        player.value:ReplacedBy::STRING AS REPLACED_BY,
        player.value:SubstitutionTime::STRING AS SubstitutionTime,
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
        'home' AS PLAYING_AS,
        -- Adding Player Status Logic
        CASE 
            WHEN STARTED_GAME = TRUE AND WAS_SUBSTITUTED = FALSE THEN 'Played Full Game'
            WHEN STARTED_GAME = TRUE AND WAS_SUBSTITUTED = TRUE THEN 'Played Subbed Off'
            WHEN STARTED_GAME = FALSE AND WAS_INTRODUCED = TRUE AND WAS_SUBSTITUTED = TRUE THEN 'Played Subbed On and Subbed Off'
            WHEN STARTED_GAME = FALSE AND WAS_INTRODUCED = TRUE AND WAS_SUBSTITUTED = FALSE THEN 'Played Subbed On'
            WHEN STARTED_GAME = FALSE AND WAS_INTRODUCED = FALSE THEN 'Did Not Play'
            ELSE 'Unknown Status'
        END AS PLAYER_STATUS
    FROM match_data m,
    LATERAL FLATTEN(input => m.match_value:home_team.players) AS player
),

away_players AS (
    SELECT
        player.key::STRING AS PLAYER_NAME,
        m.MATCH_ID,
        m.match_value:away_team.name::STRING AS TEAM_NAME,
        player.value:ShirtNumber::STRING AS TEAM_NUMBER,
        CAST(REPLACE(player.value:ShirtNumber, ',', '') AS INT ) AS TEAM_NUMBER1,       
        TRY_CAST(player.value:WasStarter::STRING AS BOOLEAN) AS STARTED_GAME,
        TRY_CAST(player.value:WasSubstituted::STRING AS BOOLEAN) AS WAS_SUBSTITUTED,
        TRY_CAST(player.value:WasIntroduced::STRING AS BOOLEAN) AS Was_Introduced,
        TRY_CAST(player.value:is_captain::STRING AS BOOLEAN) AS is_captain,
        player.value:ReplacedBy::STRING AS REPLACED_BY,
        player.value:SubstitutionTime::STRING AS SubstitutionTime,
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
        'away' AS PLAYING_AS,
        -- Adding Player Status Logic
        CASE 
            WHEN STARTED_GAME = TRUE AND WAS_SUBSTITUTED = FALSE THEN 'Played Full Game'
            WHEN STARTED_GAME = TRUE AND WAS_SUBSTITUTED = TRUE THEN 'Played Subbed Off'
            WHEN STARTED_GAME = FALSE AND WAS_INTRODUCED = TRUE AND WAS_SUBSTITUTED = TRUE THEN 'Played Subbed On and Subbed Off'
            WHEN STARTED_GAME = FALSE AND WAS_INTRODUCED = TRUE AND WAS_SUBSTITUTED = FALSE THEN 'Played Subbed On'
            WHEN STARTED_GAME = FALSE AND WAS_INTRODUCED = FALSE THEN 'Did Not Play'
            ELSE 'Unknown Status'
        END AS PLAYER_STATUS
    FROM match_data m,
    LATERAL FLATTEN(input => m.match_value:away_team.players) AS player
)

SELECT * FROM home_players
UNION ALL
SELECT * FROM away_players
