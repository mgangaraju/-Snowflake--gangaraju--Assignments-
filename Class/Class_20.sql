                                  Session-20
                                --------------------------
                               
                                    Time Travel  & File Safe
                                   
                                    https://youtu.be/6pERRWWqMr0
                                   
     Time Travel :  Is nothing but to get historical data         , suppose we have deleted/updated data
     
     
     0-90
     ----
     
     0-20 / 30
     snowflake team (7 days)
                                   
                                    Offset
                                    Timestamp
                                    query id

--------------------------------------------------------------------------------


CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string)
    


CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    


LIST @MANAGE_DB.external_stages.time_travel_stage



COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv')


SELECT * FROM OUR_FIRST_DB.public.test

ALTER TABLE test SET DATA_RETENTION_TIME_IN_DAYS=20;

show tables;

// Use-case: Update data (by mistake)

UPDATE OUR_FIRST_DB.public.test
SET FIRST_NAME = 'Joyen' 



// // // Using time travel: Method 1 - 2 minutes back
SELECT * FROM OUR_FIRST_DB.public.test at (OFFSET => -60*1.5)





select sysdate();


// // // Using time travel: Method 2 - before timestamp
SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2024-08-15 05:50:57.194'::timestamp)



UPDATE OUR_FIRST_DB.public.test
SET Job = 'Data Scientist'


SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2024-04-16 07:30:47.145'::timestamp)








// // // Using time travel: Method 3 - before Query ID

// Preparing table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Phone string,
  Job string)

COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv')
force=true

SELECT * FROM OUR_FIRST_DB.public.test


// Altering table (by mistake)
UPDATE OUR_FIRST_DB.public.test
SET EMAIL = null

delete from OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test

SELECT * FROM OUR_FIRST_DB.public.test before (statement => '01b65c2c-0001-29c3-0005-663e00015a8e')


drop table OUR_FIRST_DB.public.test;

undrop table OUR_FIRST_DB.public.test;
ALTER TABLE test SET DATA_RETENTION_TIME_IN_DAYS=30;
