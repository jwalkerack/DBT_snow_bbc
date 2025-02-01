WITH match_data AS (
    SELECT 
        match_json.key::STRING AS MATCH_ID, 
        match_json.value AS match_value
    FROM {{ source('raw_match', 'json_load_raw') }},
    LATERAL FLATTEN(input => json_data) AS match_json
),

team_data AS (
    SELECT 
        m.MATCH_ID,
        team.key::STRING AS TEAM_TYPE,  -- 'home_team' or 'away_team'
        team.value:name::STRING AS TEAM_NAME,
        team.value:manager::STRING AS TEAM_MANAGER,
        team.value:formation::STRING AS TEAM_FORMATION,
        TRY_TO_NUMBER(team.value:score::STRING) AS TEAM_SCORE,
        TRY_TO_NUMBER(REPLACE(team.value:possession::STRING, '%', '')) / 100 AS TEAM_POSSESSION
    FROM match_data m,
    LATERAL FLATTEN(input => m.match_value) AS team
    WHERE team.key IN ('home_team', 'away_team')
),

match_summary AS (
    SELECT
        t.MATCH_ID,
        
        -- Home team details
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_NAME END) AS HOME_TEAM_NAME,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_MANAGER END) AS HOME_TEAM_MANAGER,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_FORMATION END) AS HOME_TEAM_FORMATION,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_SCORE END) AS HOME_TEAM_SCORE,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_POSSESSION END) AS HOME_TEAM_POSSESSION,
        
        -- Away team details
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_NAME END) AS AWAY_TEAM_NAME,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_MANAGER END) AS AWAY_TEAM_MANAGER,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_FORMATION END) AS AWAY_TEAM_FORMATION,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_SCORE END) AS AWAY_TEAM_SCORE,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_POSSESSION END) AS AWAY_TEAM_POSSESSION,

        -- Was the game postponed?
        MAX(CASE 
            WHEN t.TEAM_TYPE IN ('home_team', 'away_team') AND t.TEAM_SCORE IS NULL THEN TRUE 
            ELSE FALSE 
        END) AS WAS_GAME_POSTPONED,

        -- Additional match details
        MAX(TRY_TO_DATE(m.match_value:played_on::STRING, 'Dy DD Mon YYYY')) AS PLAYED_ON,
        MAX(m.match_value:League_Name::STRING) AS LEAGUE_NAME,
        MAX(m.match_value:venue::STRING) AS VENUE,
        MAX(TRY_TO_NUMBER(REPLACE(m.match_value:attendance::STRING, ',', ''))) AS ATTENDANCE

    FROM team_data t
    JOIN match_data m ON t.MATCH_ID = m.MATCH_ID
    GROUP BY t.MATCH_ID
)

SELECT * FROM match_summary
