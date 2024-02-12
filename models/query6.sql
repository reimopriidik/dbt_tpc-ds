/*
List all the states with at least 10 customers who during a given month bought items with the price tag at least 20% higher than the average price of items in the same category.
Qualification Substitution Parameters:
• MONTH.01=1
• YEAR.01=2001
*/

/*
Using store sales data and store location state
*/

WITH customers_ AS (
    SELECT C_CUSTOMER_SK, C_CURRENT_ADDR_SK
    FROM {{ source('snowflake_sample_data', 'CUSTOMER') }}
    WHERE C_CURRENT_ADDR_SK IS NOT NULL
),
store_sales_ AS (
    SELECT SS_SOLD_DATE_SK, SS_CUSTOMER_SK, SS_ITEM_SK, SS_STORE_SK
    FROM {{ source('snowflake_sample_data', 'STORE_SALES') }}
    WHERE SS_SOLD_DATE_SK IS NOT NULL AND SS_ITEM_SK IS NOT NULL
),
store_ AS (
    SELECT S_STORE_SK, S_STATE
    FROM {{ source('snowflake_sample_data', 'STORE') }}
    WHERE S_STATE IS NOT NULL
),
date_ AS (
    SELECT D_DATE_SK, D_YEAR, D_MOY
    FROM {{ source('snowflake_sample_data', 'DATE_DIM') }}
),
items_ AS (
    SELECT I_ITEM_SK, I_CATEGORY_ID, I_CURRENT_PRICE
    FROM {{ source('snowflake_sample_data', 'ITEM') }}
    WHERE I_CATEGORY_ID IS NOT NULL
),
-- joining all tables
joined_table AS (
    SELECT *
    FROM store_sales_
    JOIN customers_
        ON C_CUSTOMER_SK = SS_CUSTOMER_SK
    JOIN date_
        ON SS_SOLD_DATE_SK = D_DATE_SK
    JOIN items_
        ON SS_ITEM_SK = I_ITEM_SK
    JOIN store_
        ON SS_STORE_SK = S_STORE_SK
),


-- table to calculate the average price of items per category
average_category_price AS (
    SELECT
        I_CATEGORY_ID,
        AVG(I_CURRENT_PRICE) AS ave_cat_price
    FROM items_
    GROUP BY I_CATEGORY_ID
),
-- items that are priced at least 20% higher than the category average
top20_priced_items_per_cat AS (
    SELECT
        i.I_ITEM_SK,
        i.I_CATEGORY_ID,
        i.I_CURRENT_PRICE,
        ave_cp.ave_cat_price
    FROM items_ AS i
    JOIN average_category_price AS ave_cp
        ON i.I_CATEGORY_ID = ave_cp.I_CATEGORY_ID
    WHERE i.I_CURRENT_PRICE >= 1.2 * ave_cp.ave_cat_price
),

-- APPLYING SELECTION CRITERIA
-- keeping only data from 01.2001
filter_monthyear AS (
    SELECT * FROM joined_table
    WHERE
        D_YEAR = '2001' AND D_MOY = '1'
),
-- removing items that are not price higher than 20% of category average
filter_top20_items AS (
    SELECT
        f_d.S_STATE,
        f_d.C_CUSTOMER_SK
    FROM filter_monthyear AS f_d
    JOIN top20_priced_items_per_cat AS f_i  -- using inner join to remove items that are are not priced higher
        ON f_d.SS_ITEM_SK = f_i.I_ITEM_SK
),

-- counting customers per state from filtered/cleaned data
customers_per_state AS (
    SELECT
        S_STATE,
        COUNT(DISTINCT C_CUSTOMER_SK) AS customers_per_state
    FROM filter_top20_items
    GROUP BY S_STATE
),
-- only keeping the states with purchases from at least 10 customers of interest (01.2001, 20% higher priced items)
ten_or_more_customers AS (
    SELECT S_STATE
    FROM customers_per_state
    WHERE customers_per_state >= 10
)


SELECT TOP 100 *
FROM ten_or_more_customers
ORDER BY S_STATE
-- "Finished running 1 table model in 0 hours 0 minutes and 20.92 seconds (20.92s)."