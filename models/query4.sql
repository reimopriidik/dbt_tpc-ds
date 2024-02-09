/*
B.4 query4.tpl
Find customers who spend more money via catalog than in stores. Identify preferred customers and their country of origin.
Qualification Substitution Parameters:
• YEAR.01=2001
• SELECTONE.01= t_s_secyear.customer_preferred_cust_flag
*/

/*
cannot use CS_NET_PROFIT as it depends on the cost margin
using CS_NET_PAID_INC_SHIP_TAX and assuming it is the final amount that customer paid
identifying customer as invoice addressee (CS_BILL_CUSTOMER_SK) as the first choice but if NULL then using shipping address (CS_SHIP_CUSTOMER_SK) instead
*/




WITH customers_ AS (
    SELECT C_CUSTOMER_SK, C_PREFERRED_CUST_FLAG, C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_MONTH, C_BIRTH_DAY
    FROM {{ source('snowflake_sample_data', 'CUSTOMER') }}
),
catalog_sales_ AS (
    SELECT CS_SOLD_DATE_SK, CS_BILL_CUSTOMER_SK, CS_SHIP_CUSTOMER_SK, CS_NET_PAID_INC_SHIP_TAX
    FROM {{ source('snowflake_sample_data', 'CATALOG_SALES') }}
),
store_sales_ AS (
    SELECT SS_SOLD_DATE_SK, SS_CUSTOMER_SK, SS_NET_PAID_INC_TAX
    FROM {{ source('snowflake_sample_data', 'STORE_SALES') }}
),
date_ AS (
    SELECT D_DATE_SK, D_YEAR, D_DATE
    FROM {{ source('snowflake_sample_data', 'DATE_DIM') }}
),



-- Cleaned catalog sales table from 2001
cleaned_catalog_sales_ AS (
    SELECT
        IFNULL(CS_BILL_CUSTOMER_SK, CS_SHIP_CUSTOMER_SK)    cs_customer_sk,
        CS_NET_PAID_INC_SHIP_TAX                            cs_customer_paid
    FROM catalog_sales_
    JOIN date_
        ON cs_sold_date_sk = D_DATE_SK
    WHERE cs_customer_sk IS NOT NULL AND cs_sold_date_sk IS NOT NULL AND -- removing all incomplete datasets
        D_YEAR = '2001' -- keeping only data from year 2001
),
-- Total catalog sales per customer from 2001
sum_catalog_sales_ AS (
    SELECT
        cs_customer_sk,
        SUM(cs_customer_paid)   cs_customer_paid_total
    FROM cleaned_catalog_sales_
    GROUP BY cs_customer_sk
), -- Finished running 1 table model in 0 hours 5 minutes and 9.51 seconds (309.51s).




-- Cleaned store sales table from 2001
cleaned_store_sales_ AS (
    SELECT
        SS_CUSTOMER_SK          ss_customer_sk,
        SS_NET_PAID_INC_TAX     ss_customer_paid
    FROM store_sales_
    JOIN date_
        ON ss_sold_date_sk = D_DATE_SK
    WHERE ss_customer_sk IS NOT NULL AND ss_sold_date_sk IS NOT NULL AND ss_customer_paid IS NOT NULL -- removing all incomplete datasets
        AND D_YEAR = '2001' -- keeping only data from year 2001
),
-- Total store sales per customer from 2001
sum_store_sales_ AS (
    SELECT
        ss_customer_sk,
        SUM(ss_customer_paid)   ss_customer_paid_total
    FROM cleaned_store_sales_
    GROUP BY ss_customer_sk
),


-- Joined table containing per preferred customer sales from catalog and stores in 2001
total_sales_ AS (
    SELECT
        cs_customer_sk,
        cs_customer_paid_total,
        ss_customer_paid_total,
        (cs_customer_paid_total / ss_customer_paid_total)       catalog_store_ratio,
        C_BIRTH_COUNTRY                                         origin_country,
        C_SALUTATION, C_FIRST_NAME, C_LAST_NAME, C_EMAIL_ADDRESS, C_BIRTH_COUNTRY, C_BIRTH_YEAR, C_BIRTH_MONTH, C_BIRTH_DAY
    FROM sum_catalog_sales_
    LEFT JOIN sum_store_sales_ -- join catalog (LEFT) and store sales so all data from catalog sales table would remain as we are looking for customers who purchased more from catalog. we do not care about customers who bought from store but not from catalog
        ON cs_customer_sk = ss_customer_sk
    JOIN customers_
        ON cs_customer_sk = C_CUSTOMER_SK
    WHERE
        C_PREFERRED_CUST_FLAG = 'Y' -- keeping only data from preferred customers
)


SELECT
    /*TOP 100 -- (only showing TOP 100 customers based on SR_RETURN_AMT)*/
    *
FROM total_sales_
WHERE cs_customer_paid_total > ss_customer_paid_total
ORDER BY catalog_store_ratio DESC
-- Finished running 1 table model in 0 hours 6 minutes and 46.33 seconds (406.33s).