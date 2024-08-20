                                   Session-17
                                --------------------------
                                          Snow Pipe
                               
                                https://youtu.be/25khzW6hw4E
                               
                                Data loading
                               
                                Bulk loading -->  AWS bucket
                                                  IAM
                                                  Storage integration
                                                  Copy command to load the data (Manual)
                               
                               
                                continuous loading
                               
                                           --> AWS bucket  
                                               IAM
                                               Storage integration
                                               Create pipe (Copy command)


        ----------------------------------------------

        // Create storage integration object

create or replace storage integration s3_int_pipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::025066273338:role/snow-pipe-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snow-emp-data/csv/')
   COMMENT = 'This is to practice pipe concept'

// See storage integration properties to fetch external_id so we can update it in S3

   DESC integration s3_int_pipe;


// Create table first

 CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING

  )

// Create file format object

CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;


// Create stage object with integration object & file format object

CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = ' s3://snow-emp-data/csv/'
    STORAGE_INTEGRATION = s3_int_pipe
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat
   

// Create stage object with integration object & file format object

 LIST @MANAGE_DB.external_stages.csv_folder     


// Create schema to keep things organized

 CREATE OR REPLACE SCHEMA MANAGE_DB.pipes


// Define pipe

CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.csv_folder 


// Describe pipe

DESC pipe employee_pipe


SELECT * FROM OUR_FIRST_DB.PUBLIC.employees    

ALTER pipe employee_pipe refresh;




-----------------------------------

-- Manage pipes -- 

DESC pipe MANAGE_DB.pipes.employee_pipe;

SHOW PIPES;

SHOW PIPES like '%employee%'

SHOW PIPES in database MANAGE_DB

SHOW PIPES in schema MANAGE_DB.pipes

SHOW PIPES like '%employee%' in Database MANAGE_DB


-- Changing pipe (alter stage or file format) --


// Pause pipe
ALTER PIPE MANAGE_DB.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = true

ALTER PIPE MANAGE_DB.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = false
 
// Verify pipe is paused and has pendingFileCount 0 
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.pipes.employee_pipe') 
 
 // Recreate the pipe to change the COPY statement in the definition
CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO OUR_FIRST_DB.PUBLIC.employees2
FROM @MANAGE_DB.external_stages.csv_folder  

ALTER PIPE  MANAGE_DB.pipes.employee_pipe refresh



// Resume pipe
ALTER PIPE MANAGE_DB.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = false

// Verify pipe is running again
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.pipes.employee_pipe') 



-----------------------------------------------------------------


// Handling errors


// Create file format object chane delimiter and refresh then observe 
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = '|'
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.employees   

ALTER PIPE employee_pipe refresh
 
// Validate pipe is actually working
SELECT SYSTEM$PIPE_STATUS('employee_pipe')

// Snowpipe error message
SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'MANAGE_DB.pipes.employee_pipe',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())))

    
select sysdate();
// COPY command history from table to see error massage

SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'OUR_FIRST_DB.PUBLIC.EMPLOYEES',
   START_TIME =>DATEADD(HOUR,-2,CURRENT_TIMESTAMP())))


------   


// Storing rejected /failed results in a table

CREATE OR REPLACE TABLE rejected AS 
select rejected_record from table(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'MANAGE_DB.pipes.employee_pipe',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())))


INSERT INTO rejected
select rejected_record from table(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'MANAGE_DB.pipes.employee_pipe',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())))

    SELECT * FROM rejected;



    SELECT REJECTED_RECORD FROM rejected;


   CREATE OR REPLACE TABLE rejected_values as
SELECT 
SPLIT_PART(rejected_record,',',1) as ID, 
SPLIT_PART(rejected_record,',',2) as first_name, 
SPLIT_PART(rejected_record,',',3) as last_name, 
SPLIT_PART(rejected_record,',',4) as email, 
SPLIT_PART(rejected_record,',',5) as location, 
SPLIT_PART(rejected_record,',',6) as department
FROM rejected; 



insert into OUR_FIRST_DB.PUBLIC.employees (

SELECT * FROM rejected_values)




                                       