/*
B.1 query1.tpl
Find customers who have returned items more than 20% more often than the average customer returns for a store in a given state for a given year.
Qualification Substitution Parameters:
• YEAR.01=2000
• STATE.01=TN
• AGG_FIELD.01 = SR_RETURN_AMT
*/

/*
Defining locations of relevant columns and tables

YEAR
    in STORE_RETURNS table as ST_RETRUNED_DATE_SK column
    in DATE_DIM table (D_DATE_SK) as D_YEAR column


STATE
    in CUSTOMER_ADDRESS table as CA_STATE column
    in STORE table as S_STATE column

SR_RETURN_AMT
    in STORE_RETURNS table as SR_RETURN_AMT column
*/

WITH customers_ AS (
  SELECT C_CUSTOMER_SK, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_MONTH
  FROM {{ source('snowflake_sample_data', 'CUSTOMER') }}
),
store_returns_ AS (
  SELECT
    SR_RETURNED_DATE_SK, SR_RETURN_TIME_SK, SR_ITEM_SK, SR_CUSTOMER_SK, SR_STORE_SK, SR_RETURN_AMT
  FROM {{ source('snowflake_sample_data', 'STORE_RETURNS') }}
),
date_ AS (
  SELECT D_DATE_SK, D_YEAR, D_DATE
  FROM {{ source('snowflake_sample_data', 'DATE_DIM') }}
),
state_ AS (
  SELECT S_STORE_SK, S_STATE
  FROM {{ source('snowflake_sample_data', 'STORE') }}
),
-- Summarised table for all customer returns in 2000 in TN state
sum_table AS (
  SELECT
    SR_CUSTOMER_SK, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_MONTH, D_DATE, SR_RETURN_AMT, SR_ITEM_SK,
    SUM(SR_RETURN_AMT) OVER(PARTITION BY SR_CUSTOMER_SK) AS R_AMT_sum -- Column to sum all returns per customer
  FROM store_returns_ AS sr
  JOIN customers_ AS c
    ON sr.SR_CUSTOMER_SK = c.C_CUSTOMER_SK
  JOIN date_ AS d
    ON sr.SR_RETURNED_DATE_SK = d.D_DATE_SK
  JOIN state_ AS s
    ON sr.SR_STORE_SK = s.S_STORE_SK
  WHERE
    d.D_YEAR = '2000' AND
    s.S_STATE = 'TN' AND
    sr.SR_RETURN_AMT IS NOT NULL
  GROUP BY 
    SR_CUSTOMER_SK, SR_RETURNED_DATE_SK, SR_RETURN_TIME_SK, SR_ITEM_SK,
    SR_STORE_SK, SR_RETURN_AMT, C_CUSTOMER_SK, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_MONTH, C_BIRTH_YEAR, D_DATE_SK, D_YEAR, D_DATE, S_STORE_SK, S_STATE
),
-- Condenced table showing returns per each customer
customer_total_return AS (
  SELECT SR_CUSTOMER_SK, R_AMT_sum
  FROM sum_table
  GROUP BY SR_CUSTOMER_SK, R_AMT_sum
),
-- Average returns in 2000 in TN state
return_AMT_ave AS (
  SELECT AVG(R_AMT_sum)
  FROM customer_total_return
)

-- Final result table
-- (only showing TOP 100 customers based on SR_RETURN_AMT)
SELECT
  TOP 100
  SR_CUSTOMER_SK, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_MONTH, R_AMT_sum
FROM sum_table
WHERE R_AMT_sum > 1.2 * (SELECT * FROM return_AMT_ave)
GROUP BY SR_CUSTOMER_SK, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_MONTH, R_AMT_sum
ORDER BY R_AMT_sum DESC
-- "Finished running 1 table model in 0 hours 1 minutes and 18.25 seconds (78.25s)."






-- SELECT * FROM return_AMT_ave

/*
-- Final result table
SELECT *
FROM sum_table
WHERE R_AMT_sum > 1.2 * (SELECT * FROM return_AMT_ave)
ORDER BY R_AMT_sum, SR_CUSTOMER_SK, D_DATE
*/