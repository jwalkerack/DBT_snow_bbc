{{ config(
    materialized='table'
) }}

SELECT * FROM {{ ref('stg_match_results') }}