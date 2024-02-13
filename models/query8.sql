/*
Compute the net profit of stores located in 400 Metropolitan areas with more than 10 preferred customers.
Qualification Substitution Parameters:
• ZIP.01=24128 ZIP.81=57834 ZIP.161=13354 ZIP.241=15734 ZIP.321=78668
• ZIP.02=76232 ZIP.82=62878 ZIP.162=45375 ZIP.242=63435 ZIP.322=22245
• ZIP.03=65084 ZIP.83=49130 ZIP.163=40558 ZIP.243=25733 ZIP.323=15798
• ZIP.04=87816 ZIP.84=81096 ZIP.164=56458 ZIP.244=35474 ZIP.324=27156
• ZIP.05=83926 ZIP.85=18840 ZIP.165=28286 ZIP.245=24676 ZIP.325=37930
• ZIP.06=77556 ZIP.86=27700 ZIP.166=45266 ZIP.246=94627 ZIP.326=62971
• ZIP.07=20548 ZIP.87=23470 ZIP.167=47305 ZIP.247=53535 ZIP.327=21337
• ZIP.08=26231 ZIP.88=50412 ZIP.168=69399 ZIP.248=17879 ZIP.328=51622
• ZIP.09=43848 ZIP.89=21195 ZIP.169=83921 ZIP.249=15559 ZIP.329=67853
• ZIP.10=15126 ZIP.90=16021 ZIP.170=26233 ZIP.250=53268 ZIP.330=10567
• ZIP.11=91137 ZIP.91=76107 ZIP.171=11101 ZIP.251=59166 ZIP.331=38415
• ZIP.12=61265 ZIP.92=71954 ZIP.172=15371 ZIP.252=11928 ZIP.332=15455
• ZIP.13=98294 ZIP.93=68309 ZIP.173=69913 ZIP.253=59402 ZIP.333=58263
• ZIP.14=25782 ZIP.94=18119 ZIP.174=35942 ZIP.254=33282 ZIP.334=42029
• ZIP.15=17920 ZIP.95=98359 ZIP.175=15882 ZIP.255=45721 ZIP.335=60279
• ZIP.16=18426 ZIP.96=64544 ZIP.176=25631 ZIP.256=43933 ZIP.336=37125
• ZIP.17=98235 ZIP.97=10336 ZIP.177=24610 ZIP.257=68101 ZIP.337=56240
• ZIP.18=40081 ZIP.98=86379 ZIP.178=44165 ZIP.258=33515 ZIP.338=88190
• ZIP.19=84093 ZIP.99=27068 ZIP.179=99076 ZIP.259=36634 ZIP.339=50308
• ZIP.20=28577 ZIP.100=39736 ZIP.180=33786 ZIP.260=71286 ZIP.340=26859
• ZIP.21=55565 ZIP.101=98569 ZIP.181=70738 ZIP.261=19736 ZIP.341=64457
• ZIP.22=17183 ZIP.102=28915 ZIP.182=26653 ZIP.262=58058 ZIP.342=89091
• ZIP.23=54601 ZIP.103=24206 ZIP.183=14328 ZIP.263=55253 ZIP.343=82136
• ZIP.24=67897 ZIP.104=56529 ZIP.184=72305 ZIP.264=67473 ZIP.344=62377
• ZIP.25=22752 ZIP.105=57647 ZIP.185=62496 ZIP.265=41918 ZIP.345=36233
• ZIP.26=86284 ZIP.106=54917 ZIP.186=22152 ZIP.266=19515 ZIP.346=63837
• ZIP.27=18376 ZIP.107=42961 ZIP.187=10144 ZIP.267=36495 ZIP.347=58078
• ZIP.28=38607 ZIP.108=91110 ZIP.188=64147 ZIP.268=19430 ZIP.348=17043
• ZIP.29=45200 ZIP.109=63981 ZIP.189=48425 ZIP.269=22351 ZIP.349=30010
• ZIP.30=21756 ZIP.110=14922 ZIP.190=14663 ZIP.270=77191 ZIP.350=60099
• ZIP.31=29741 ZIP.111=36420 ZIP.191=21076 ZIP.271=91393 ZIP.351=28810
• ZIP.32=96765 ZIP.112=23006 ZIP.192=18799 ZIP.272=49156 ZIP.352=98025
• ZIP.33=23932 ZIP.113=67467 ZIP.193=30450 ZIP.273=50298 ZIP.353=29178
• ZIP.34=89360 ZIP.114=32754 ZIP.194=63089 ZIP.274=87501 ZIP.354=87343
• ZIP.35=29839 ZIP.115=30903 ZIP.195=81019 ZIP.275=18652 ZIP.355=73273
• ZIP.36=25989 ZIP.116=20260 ZIP.196=68893 ZIP.276=53179 ZIP.356=30469
• ZIP.37=28898 ZIP.117=31671 ZIP.197=24996 ZIP.277=18767 ZIP.357=64034
• ZIP.38=91068 ZIP.118=51798 ZIP.198=51200 ZIP.278=63193 ZIP.358=39516
• ZIP.39=72550 ZIP.119=72325 ZIP.199=51211 ZIP.279=23968 ZIP.359=86057
• ZIP.40=10390 ZIP.120=85816 ZIP.200=45692 ZIP.280=65164 ZIP.360=21309
• ZIP.41=18845 ZIP.121=68621 ZIP.201=92712 ZIP.281=68880 ZIP.361=90257
• ZIP.42=47770 ZIP.122=13955 ZIP.202=70466 ZIP.282=21286 ZIP.362=67875
• ZIP.43=82636 ZIP.123=36446 ZIP.203=79994 ZIP.283=72823 ZIP.363=40162
• ZIP.44=41367 ZIP.124=41766 ZIP.204=22437 ZIP.284=58470 ZIP.364=11356
• ZIP.45=76638 ZIP.125=68806 ZIP.205=25280 ZIP.285=67301 ZIP.365=73650
• ZIP.46=86198 ZIP.126=16725 ZIP.206=38935 ZIP.286=13394 ZIP.366=61810
• ZIP.47=81312 ZIP.127=15146 ZIP.207=71791 ZIP.287=31016 ZIP.367=72013
• ZIP.48=37126 ZIP.128=22744 ZIP.208=73134 ZIP.288=70372 ZIP.368=30431
• ZIP.49=39192 ZIP.129=35850 ZIP.209=56571 ZIP.289=67030 ZIP.369=22461
• ZIP.50=88424 ZIP.130=88086 ZIP.210=14060 ZIP.290=40604 ZIP.370=19512
• ZIP.51=72175 ZIP.131=51649 ZIP.211=19505 ZIP.291=24317 ZIP.371=13375
• ZIP.52=81426 ZIP.132=18270 ZIP.212=72425 ZIP.292=45748 ZIP.372=55307
• ZIP.53=53672 ZIP.133=52867 ZIP.213=56575 ZIP.293=39127 ZIP.373=30625
• ZIP.54=10445 ZIP.134=39972 ZIP.214=74351 ZIP.294=26065 ZIP.374=83849
• ZIP.55=42666 ZIP.135=96976 ZIP.215=68786 ZIP.295=77721 ZIP.375=68908
• ZIP.56=66864 ZIP.136=63792 ZIP.216=51650 ZIP.296=31029 ZIP.376=26689
• ZIP.57=66708 ZIP.137=11376 ZIP.217=20004 ZIP.297=31880 ZIP.377=96451
• ZIP.58=41248 ZIP.138=94898 ZIP.218=18383 ZIP.298=60576 ZIP.378=38193
• ZIP.59=48583 ZIP.139=13595 ZIP.219=76614 ZIP.299=24671 ZIP.379=46820
• ZIP.60=82276 ZIP.140=10516 ZIP.220=11634 ZIP.300=45549 ZIP.380=88885
• ZIP.61=18842 ZIP.141=90225 ZIP.221=18906 ZIP.301=13376 ZIP.381=84935
• ZIP.62=78890 ZIP.142=58943 ZIP.222=15765 ZIP.302=50016 ZIP.382=69035
• ZIP.63=49448 ZIP.143=39371 ZIP.223=41368 ZIP.303=33123 ZIP.383=83144
• ZIP.64=14089 ZIP.144=94945 ZIP.224=73241 ZIP.304=19769 ZIP.384=47537
• ZIP.65=38122 ZIP.145=28587 ZIP.225=76698 ZIP.305=22927 ZIP.385=56616
• ZIP.66=34425 ZIP.146=96576 ZIP.226=78567 ZIP.306=97789 ZIP.386=94983
• ZIP.67=79077 ZIP.147=57855 ZIP.227=97189 ZIP.307=46081 ZIP.387=48033
• ZIP.68=19849 ZIP.148=28488 ZIP.228=28545 ZIP.308=72151 ZIP.388=69952
• ZIP.69=43285 ZIP.149=26105 ZIP.229=76231 ZIP.309=15723 ZIP.389=25486
• ZIP.70=39861 ZIP.150=83933 ZIP.230=75691 ZIP.310=46136 ZIP.390=61547
• ZIP.71=66162 ZIP.151=25858 ZIP.231=22246 ZIP.311=51949 ZIP.391=27385
• ZIP.72=77610 ZIP.152=34322 ZIP.232=51061 ZIP.312=68100 ZIP.392=61860
• ZIP.73=13695 ZIP.153=44438 ZIP.233=90578 ZIP.313=96888 ZIP.393=58048
• ZIP.74=99543 ZIP.154=73171 ZIP.234=56691 ZIP.314=64528 ZIP.394=56910
• ZIP.75=83444 ZIP.155=30122 ZIP.235=68014 ZIP.315=14171 ZIP.395=16807
• ZIP.76=83041 ZIP.156=34102 ZIP.236=51103 ZIP.316=79777 ZIP.396=17871
• ZIP.77=12305 ZIP.157=22685 ZIP.237=94167 ZIP.317=28709 ZIP.397=35258
• ZIP.78=57665 ZIP.158=71256 ZIP.238=57047 ZIP.318=11489 ZIP.398=31387
• ZIP.79=68341 ZIP.159=78451 ZIP.239=14867 ZIP.319=25103 ZIP.399=35458
• ZIP.80=25003 ZIP.160=54364 ZIP.240=73520 ZIP.320=32213 ZIP.400=35576
• QOY.01=2
• YEAR.01=1998
*/

/*
Using MS Excel to clean and concate the list of ZIP codes onto one line:
'24128','76232','65084','87816','83926','77556','20548','26231','43848','15126','91137','61265','98294','25782','17920','18426','98235','40081','84093','28577','55565','17183','54601','67897','22752','86284','18376','38607','45200','21756','29741','96765','23932','89360','29839','25989','28898','91068','72550','10390','18845','47770','82636','41367','76638','86198','81312','37126','39192','88424','72175','81426','53672','10445','42666','66864','66708','41248','48583','82276','18842','78890','49448','14089','38122','34425','79077','19849','43285','39861','66162','77610','13695','99543','83444','83041','12305','57665','68341','25003','57834','62878','49130','81096','18840','27700','23470','50412','21195','16021','76107','71954','68309','18119','98359','64544','10336','86379','27068','39736','98569','28915','24206','56529','57647','54917','42961','91110','63981','14922','36420','23006','67467','32754','30903','20260','31671','51798','72325','85816','68621','13955','36446','41766','68806','16725','15146','22744','35850','88086','51649','18270','52867','39972','96976','63792','11376','94898','13595','10516','90225','58943','39371','94945','28587','96576','57855','28488','26105','83933','25858','34322','44438','73171','30122','34102','22685','71256','78451','54364','13354','45375','40558','56458','28286','45266','47305','69399','83921','26233','11101','15371','69913','35942','15882','25631','24610','44165','99076','33786','70738','26653','14328','72305','62496','22152','10144','64147','48425','14663','21076','18799','30450','63089','81019','68893','24996','51200','51211','45692','92712','70466','79994','22437','25280','38935','71791','73134','56571','14060','19505','72425','56575','74351','68786','51650','20004','18383','76614','11634','18906','15765','41368','73241','76698','78567','97189','28545','76231','75691','22246','51061','90578','56691','68014','51103','94167','57047','14867','73520','15734','63435','25733','35474','24676','94627','53535','17879','15559','53268','59166','11928','59402','33282','45721','43933','68101','33515','36634','71286','19736','58058','55253','67473','41918','19515','36495','19430','22351','77191','91393','49156','50298','87501','18652','53179','18767','63193','23968','65164','68880','21286','72823','58470','67301','13394','31016','70372','67030','40604','24317','45748','39127','26065','77721','31029','31880','60576','24671','45549','13376','50016','33123','19769','22927','97789','46081','72151','15723','46136','51949','68100','96888','64528','14171','79777','28709','11489','25103','32213','78668','22245','15798','27156','37930','62971','21337','51622','67853','10567','38415','15455','58263','42029','60279','37125','56240','88190','50308','26859','64457','89091','82136','62377','36233','63837','58078','17043','30010','60099','28810','98025','29178','87343','73273','30469','64034','39516','86057','21309','90257','67875','40162','11356','73650','61810','72013','30431','22461','19512','13375','55307','30625','83849','68908','26689','96451','38193','46820','88885','84935','69035','83144','47537','56616','94983','48033','69952','25486','61547','27385','61860','58048','56910','16807','17871','35258','31387','35458','35576'
*/

WITH customers_ AS (
    SELECT C_CUSTOMER_SK, C_PREFERRED_CUST_FLAG
    FROM {{ source('snowflake_sample_data', 'CUSTOMER') }}
),
store_sales_ AS (
    SELECT SS_SOLD_DATE_SK, SS_CUSTOMER_SK, SS_STORE_SK, SS_NET_PROFIT
    FROM {{ source('snowflake_sample_data', 'STORE_SALES') }}
),
store_ AS (
    SELECT
        S_STORE_SK, S_ZIP,
        S_STORE_NAME, S_NUMBER_EMPLOYEES, S_MANAGER, S_STREET_NUMBER, S_STREET_NAME, S_STREET_TYPE, S_CITY, S_COUNTY
    FROM {{ source('snowflake_sample_data', 'STORE') }}
    WHERE S_STATE IS NOT NULL
),
date_ AS (
    SELECT D_DATE_SK, D_YEAR, D_QOY
    FROM {{ source('snowflake_sample_data', 'DATE_DIM') }}
),
joined_table AS (
    SELECT *
    FROM store_sales_
    JOIN customers_
        ON SS_CUSTOMER_SK = C_CUSTOMER_SK
    JOIN store_
        ON SS_STORE_SK = S_STORE_SK
    JOIN date_
        on SS_SOLD_DATE_SK = D_DATE_SK
),
filtered_table AS (
    SELECT *
    FROM joined_table
    WHERE
        D_YEAR = '1998' AND D_QOY = '2' AND
        C_PREFERRED_CUST_FLAG = 'Y' AND
        S_ZIP IN (
            '24128','76232','65084','87816','83926','77556','20548','26231','43848','15126','91137','61265','98294','25782','17920','18426','98235','40081','84093','28577',
            '55565','17183','54601','67897','22752','86284','18376','38607','45200','21756','29741','96765','23932','89360','29839','25989','28898','91068','72550','10390',
            '18845','47770','82636','41367','76638','86198','81312','37126','39192','88424','72175','81426','53672','10445','42666','66864','66708','41248','48583','82276',
            '18842','78890','49448','14089','38122','34425','79077','19849','43285','39861','66162','77610','13695','99543','83444','83041','12305','57665','68341','25003',
            '57834','62878','49130','81096','18840','27700','23470','50412','21195','16021','76107','71954','68309','18119','98359','64544','10336','86379','27068','39736',
            '98569','28915','24206','56529','57647','54917','42961','91110','63981','14922','36420','23006','67467','32754','30903','20260','31671','51798','72325','85816',
            '68621','13955','36446','41766','68806','16725','15146','22744','35850','88086','51649','18270','52867','39972','96976','63792','11376','94898','13595','10516',
            '90225','58943','39371','94945','28587','96576','57855','28488','26105','83933','25858','34322','44438','73171','30122','34102','22685','71256','78451','54364',
            '13354','45375','40558','56458','28286','45266','47305','69399','83921','26233','11101','15371','69913','35942','15882','25631','24610','44165','99076','33786',
            '70738','26653','14328','72305','62496','22152','10144','64147','48425','14663','21076','18799','30450','63089','81019','68893','24996','51200','51211','45692',
            '92712','70466','79994','22437','25280','38935','71791','73134','56571','14060','19505','72425','56575','74351','68786','51650','20004','18383','76614','11634',
            '18906','15765','41368','73241','76698','78567','97189','28545','76231','75691','22246','51061','90578','56691','68014','51103','94167','57047','14867','73520',
            '15734','63435','25733','35474','24676','94627','53535','17879','15559','53268','59166','11928','59402','33282','45721','43933','68101','33515','36634','71286',
            '19736','58058','55253','67473','41918','19515','36495','19430','22351','77191','91393','49156','50298','87501','18652','53179','18767','63193','23968','65164',
            '68880','21286','72823','58470','67301','13394','31016','70372','67030','40604','24317','45748','39127','26065','77721','31029','31880','60576','24671','45549',
            '13376','50016','33123','19769','22927','97789','46081','72151','15723','46136','51949','68100','96888','64528','14171','79777','28709','11489','25103','32213',
            '78668','22245','15798','27156','37930','62971','21337','51622','67853','10567','38415','15455','58263','42029','60279','37125','56240','88190','50308','26859',
            '64457','89091','82136','62377','36233','63837','58078','17043','30010','60099','28810','98025','29178','87343','73273','30469','64034','39516','86057','21309',
            '90257','67875','40162','11356','73650','61810','72013','30431','22461','19512','13375','55307','30625','83849','68908','26689','96451','38193','46820','88885',
            '84935','69035','83144','47537','56616','94983','48033','69952','25486','61547','27385','61860','58048','56910','16807','17871','35258','31387','35458','35576'
            )
),
total_net_profit AS(
    SELECT
        SS_STORE_SK, S_STORE_NAME, S_NUMBER_EMPLOYEES, S_MANAGER, S_STREET_NUMBER, S_STREET_NAME, S_STREET_TYPE, S_CITY, S_COUNTY, S_ZIP,
        SUM(SS_NET_PROFIT)  sum_net_profit
    FROM filtered_table
    GROUP BY
        SS_STORE_SK, S_STORE_NAME, S_NUMBER_EMPLOYEES, S_MANAGER, S_STREET_NUMBER, S_STREET_NAME, S_STREET_TYPE, S_CITY, S_COUNTY, S_ZIP
)

SELECT *
FROM total_net_profit
-- "Finished running 1 table model in 0 hours 0 minutes and 29.21 seconds (29.21s)."