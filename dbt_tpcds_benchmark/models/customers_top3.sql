-- TOP 3 rows of CUSTOMERS table

WITH source_data AS (
  SELECT *
  FROM {{ source('snowflake_sample_data', 'CUSTOMER') }}
  LIMIT 3
)

SELECT *
FROM source_data