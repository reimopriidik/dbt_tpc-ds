/*
Count the customers with the same gender, marital status, education status, purchase estimate, credit rating, dependent count, employed dependent count and college dependent count who live in certain counties and who have purchased
from both stores and another sales channel during a three month time period of a given year.
Qualification Substitution Parameters:
• YEAR.01 = 2002
• MONTH.01 = 1                          --probably a typo. should be QUARTER.01 = 1
• COUNTY.01 = Rush County
• COUNTY.02 = Toole County
• COUNTY.03 = Jefferson County
• COUNTY.04 = Dona Ana County
• COUNTY.05 = La Porte County
*/

WITH c_addr_ AS (
    SELECT CA_ADDRESS_SK, CA_COUNTY
    FROM {{ source('snowflake_sample_data', 'CUSTOMER_ADDRESS') }}
    WHERE
        CA_COUNTY = 'Rush County' OR
        CA_COUNTY = 'Toole County' OR
        CA_COUNTY = 'Jefferson County' OR
        CA_COUNTY = 'Dona Ana County' OR
        CA_COUNTY = 'La Porte County'
),
customers_ AS (
    SELECT C_CUSTOMER_SK, C_CURRENT_CDEMO_SK, C_CURRENT_ADDR_SK
    FROM {{ source('snowflake_sample_data', 'CUSTOMER') }}
    WHERE C_CURRENT_ADDR_SK IN (SELECT CA_ADDRESS_SK FROM c_addr_) --only keeping the data for the customers living in the counties of interest
),
demographics_ AS (
    select CD_DEMO_SK, CD_GENDER, CD_MARITAL_STATUS, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_CREDIT_RATING, CD_DEP_COUNT, CD_DEP_EMPLOYED_COUNT, CD_DEP_COLLEGE_COUNT
    FROM {{ source('snowflake_sample_data', 'CUSTOMER_DEMOGRAPHICS') }}
    WHERE CD_DEMO_SK IN (SELECT C_CURRENT_CDEMO_SK FROM customers_) --only keeping the data for the customers in the filtered customers_ table (living in the counties of interest)
),
date_ AS (
    SELECT D_DATE_SK, D_YEAR, D_QOY
    FROM {{ source('snowflake_sample_data', 'DATE_DIM') }}
    WHERE D_YEAR = '2002' and D_QOY = '1' --only date keys for Q1 of 2002
),
store_sales_ AS (
    SELECT SS_SOLD_DATE_SK, SS_CUSTOMER_SK
    FROM {{ source('snowflake_sample_data', 'STORE_SALES') }}
    WHERE SS_SOLD_DATE_SK IN (SELECT D_DATE_SK FROM date_) --keeping only store sales data from Q1 of 2002
),
catalog_sales_ AS (
    SELECT
        CS_SOLD_DATE_SK,
        IFNULL(CS_BILL_CUSTOMER_SK, CS_SHIP_CUSTOMER_SK)    CS_customer_sk
    FROM {{ source('snowflake_sample_data', 'CATALOG_SALES') }}
    WHERE CS_SOLD_DATE_SK IN (SELECT D_DATE_SK FROM date_)  --keeping only catalog sales data from Q1 of 2002
),
web_sales_ AS (
    SELECT
        WS_SOLD_DATE_SK,
        IFNULL(WS_BILL_CUSTOMER_SK, WS_SHIP_CUSTOMER_SK)    WS_customer_sk
    FROM {{ source('snowflake_sample_data', 'WEB_SALES') }}
    WHERE WS_SOLD_DATE_SK IN (SELECT D_DATE_SK FROM date_)  --keeping only web sales data from Q1 of 2002
),
store_date AS (
    SELECT
        C_CUSTOMER_SK,
        CD_GENDER, CD_MARITAL_STATUS, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_CREDIT_RATING, CD_DEP_COUNT, CD_DEP_EMPLOYED_COUNT, CD_DEP_COLLEGE_COUNT,
        CA_COUNTY,
        D_YEAR      SS_sold_year,
        D_QOY       SS_sold_quarter,
        CS_SOLD_DATE_SK, WS_SOLD_DATE_SK
    FROM customers_
    JOIN demographics_ ON C_CURRENT_CDEMO_SK = CD_DEMO_SK
    JOIN c_addr_ ON C_CURRENT_ADDR_SK = CA_ADDRESS_SK           --using inner join to remove data from other countis
    JOIN store_sales_ ON C_CUSTOMER_SK = SS_CUSTOMER_SK
    LEFT JOIN catalog_sales_ ON C_CUSTOMER_SK = CS_customer_sk  --using left join to keep all data from store sales
    LEFT JOIN web_sales_ ON C_CUSTOMER_SK = WS_customer_sk      --using left join to keep all data from store sales
    JOIN date_ ON SS_SOLD_DATE_SK = D_DATE_SK                   --using inner join to remove data that is not in the relevant timeframe
),
store_cat_date AS (
    SELECT
        C_CUSTOMER_SK,
        CD_GENDER, CD_MARITAL_STATUS, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_CREDIT_RATING, CD_DEP_COUNT, CD_DEP_EMPLOYED_COUNT, CD_DEP_COLLEGE_COUNT,
        CA_COUNTY,
        SS_sold_year, SS_sold_quarter,
        D_YEAR      CS_sold_year,
        D_QOY       CS_sold_quarter,
        WS_SOLD_DATE_SK
    FROM store_date
    JOIN date_ ON CS_SOLD_DATE_SK = D_DATE_SK      --using left join to keep all data from store sales
),
store_cat_web_date AS (
    SELECT
        C_CUSTOMER_SK,
        CD_GENDER, CD_MARITAL_STATUS, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_CREDIT_RATING, CD_DEP_COUNT, CD_DEP_EMPLOYED_COUNT, CD_DEP_COLLEGE_COUNT,
        CA_COUNTY,
        SS_sold_year, SS_sold_quarter, CS_sold_year, CS_sold_quarter,
        D_YEAR      WS_sold_year,
        D_QOY       WS_sold_quarter
    FROM store_cat_date
    JOIN date_ ON WS_SOLD_DATE_SK = D_DATE_SK       --using left join to keep all data from store sales
),
filtered_table AS (
    SELECT *
    FROM store_cat_web_date
    WHERE
        CA_COUNTY IS NOT NULL AND       -- keeping only the data for relevant counties
        -- checking if there the same customer has brought anything from store and also from web or catalog during the 1st quarter of 2002
        SS_sold_year = '2002' AND SS_sold_quarter = '1' AND
        (
            (CS_sold_year = '2002' AND CS_sold_quarter = '1')
            OR
            (WS_sold_year = '2002' AND WS_sold_quarter = '1' )
        )
),
customers_count AS (
    SELECT
        COUNT(C_CUSTOMER_SK)    unique_demographics,
        CD_GENDER, CD_MARITAL_STATUS, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_CREDIT_RATING, CD_DEP_COUNT, CD_DEP_EMPLOYED_COUNT, CD_DEP_COLLEGE_COUNT,
        CA_COUNTY
    FROM filtered_table
    GROUP BY
        CD_GENDER, CD_MARITAL_STATUS, CD_EDUCATION_STATUS, CD_PURCHASE_ESTIMATE, CD_CREDIT_RATING, CD_DEP_COUNT, CD_DEP_EMPLOYED_COUNT, CD_DEP_COLLEGE_COUNT,
        CA_COUNTY
)

SELECT *
FROM customers_count
-- "Finished running 1 table model in 0 hours 1 minutes and 8.52 seconds (68.52s)."