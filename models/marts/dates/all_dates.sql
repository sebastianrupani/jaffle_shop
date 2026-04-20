{{ config(
    materialized='table',
    meta= {'required_tests': {
             'unique': 1,
             'not_null': 1
      }
    }
) }}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2019-01-01' as date)",
    end_date="cast('2020-01-01' as date)"
   )
}}