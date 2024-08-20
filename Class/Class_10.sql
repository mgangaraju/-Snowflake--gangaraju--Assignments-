
								        Session-10
								--------------------------
								   Data Loading 
								   
								   Bulk Loading 
								   
								   Internal stages & External stages 
								


CREATE or replace TABLE OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT (
  Loan_ID STRING,
  loan_status STRING,
  Principal STRING,
  terms STRING,
  effective_date STRING,
  due_date STRING,
  paid_off_time STRING,
  past_due_days STRING,
  age STRING,
  education STRING,
  Gender STRING);


  
  create or replace database OUR_FIRST_DB;

  
  USE DATABASE OUR_FIRST_DB;


  SELECT * FROM LOAN_PAYMENT;

   //Loading the data from S3 bucket

  COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1);


   SELECT * FROM LOAN_PAYMENT;                

CREATE TABLE LOAN_PAY1 AS(
SELECT 
  LOAN_ID
,LOAN_STATUS
,PRINCIPAL
,TERMS
,EFFECTIVE_DATE
,DUE_DATE
,PAID_OFF_TIME
,PAST_DUE_DAYS
,EDUCATION
,GENDER
,AGE
,case when age > 21 then 'Major' else 'Minor' end  as "Status"
from LOAN_PAYMENT  order by age asc 
)


  SELECT * FROM LOAN_PAY1;



)

///TASK -1
-------------------

  CREATE OR REPLACE TABLE LOAN_PAY3 AS
SELECT 
  LOAN_ID,
  LOAN_STATUS,
  TERMS,
  EDUCATION,
  GENDER,
  AGE,
  CASE
    WHEN (AGE >= 21 AND GENDER = 'male') THEN 'Major'
    WHEN (AGE < 21  AND GENDER = 'male') THEN 'Minor'
    ELSE 'N/A' 
  END AS M_STATUS,
  CASE
    WHEN AGE >= 18 AND GENDER = 'female' THEN 'Major'
    WHEN (AGE < 18  AND GENDER = 'male') THEN 'Minor'
    ELSE 'N/A'
  END AS F_STATUS,
FROM 
  LOAN_PAYMENT;


  select * from loan_pay3

  
-------------------



create or replace stage OUR_FIRST_DB.PUBLIC.ext_loan_stage 
    url= 's3://bucketsnowflakes3/Loan_payments_data.csv'


    
list @OUR_FIRST_DB.PUBLIC.ext_loan_stage ;


  select 
            s.$1,
            s.$2,
            s.$9,
            s.$3
           -- case when s.$9 > 21 then 'Major' else 'Minor' end  as "Status"
          from @OUR_FIRST_DB.PUBLIC.ext_loan_stage s

          
CREATE TABLE OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_V2 (
  Loan_ID STRING,
  loan_status STRING,
  age int,
  status string);


  
COPY INTO OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_V2 (Loan_ID,loan_status,age,status)
    FROM (select 
            s.$1,
            s.$2,
            s.$9,
            case when s.$9 > 21 then 'Major' else 'Minor' end  as "Status"
          from @OUR_FIRST_DB.PUBLIC.ext_loan_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1) ;
          


select * from loan_payment_v2 ;

-------------------------------------------------------------




CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_V3 (
  Loan_ID STRING,
  LOAN_STATUS STRING,
  TERMS STRING,
  EDUCATION STRING,
  AGE int,
  GENDER STRING,
  M_STATUS STRING,
  F_STATUS STRING)




  select * from loan_payment_v3
  


COPY INTO OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_V3 (Loan_ID,LOAN_STATUS,TERMS,EDUCATION,AGE,GENDER)
    FROM (select
            s.$1,
            s.$2,
            s.$9,
            s.$4,
            s.$11,
            s.$10,
    CASE
    WHEN  (s.$9 >= 21 and s.$11 = 'male') THEN 'Major'
    WHEN  (s.$9 < 21 and s.$11 = 'male') THEN 'Minor'
    ELSE 'N/A'
  END AS M_STATUS,
  CASE
    WHEN (s.$9 >= 18 and s.$11 = 'female') THEN 'Major'
    WHEN (s.$9 < 18 and s.$11 = 'female') THEN 'Minor'
    ELSE 'N/A'
  END AS F_STATUS
  from @OUR_FIRST_DB.PUBLIC.ext_loan_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1) ;
          




  COPY INTO OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_V3 
    (Loan_ID, LOAN_STATUS, TERMS, EDUCATION, AGE, GENDER, M_STATUS, F_STATUS)
FROM (
    SELECT
        s.$1 AS Loan_ID,
        s.$2 AS LOAN_STATUS,
        s.$4 AS TERMS,
        s.$10 AS EDUCATION,
        s.$9 AS AGE,
        s.$11 AS GENDER,
        CASE
            WHEN (s.$9 >= 21 AND s.$11 = 'male') THEN 'Major'
            WHEN (s.$9 < 21 AND s.$11 = 'male') THEN 'Minor'
            ELSE 'N/A'
        END AS M_STATUS,
        CASE
            WHEN (s.$9 >= 18 AND s.$11 = 'female') THEN 'Major'
            WHEN (s.$9 < 18 AND s.$11 = 'female') THEN 'Minor'
            ELSE 'N/A'
        END AS F_STATUS
    FROM @OUR_FIRST_DB.PUBLIC.ext_loan_stage s
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

      
      
      COPY INTO OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_V3 
    (Loan_ID, LOAN_STATUS, TERMS, EDUCATION, AGE, GENDER, M_STATUS, F_STATUS)
FROM (
    SELECT
        s.$1 AS Loan_ID,
        s.$2 AS LOAN_STATUS,
        s.$4 AS TERMS,
        s.$10 AS EDUCATION,
        s.$9 AS AGE,
        s.$11 AS GENDER,
        CASE
            WHEN (s.$9 >= 21 AND s.$11 = 'male') THEN 'Major'
            WHEN (s.$9 < 21 AND s.$11 = 'male') THEN 'Minor'
            ELSE 'N/A'
        END AS M_STATUS,
        CASE
            WHEN (s.$9 >= 18 AND s.$11 = 'female') THEN 'Major'
            WHEN (s.$9 < 18 AND s.$11 = 'female') THEN 'Minor'
            ELSE 'N/A'
        END AS F_STATUS
    FROM @OUR_FIRST_DB.PUBLIC.ext_loan_stage s
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';  -- Skip problematic rows
