                                    Session-18
								--------------------------
								   Snow Pipe Error handling  
								   Azure account creation 
								   
	{"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2024-08-13T05:28:56.164Z","lastIngestedFilePath":"employee_data_error_rec.csv","notificationChannelName":"arn:aws:sqs:ap-southeast-2:211125613752:sf-snowpipe-AIDATCKARGS4LKMCXKFYP-Fdo5oMRNts3WaOtIR3J5JA","numOutstandingMessagesOnChannel":1,"lastReceivedMessageTimestamp":"2024-08-13T05:28:56.026Z","lastForwardedMessageTimestamp":"2024-08-13T05:28:56.172Z","lastPulledFromChannelTimestamp":"2024-08-13T05:29:15.869Z","lastForwardedFilePath":"snow-emp-vitech-data/csv/employee_data_error_rec.csv"}
	
	
	vitech talks azure account creation  
	
	
Azure account creation--->	https://www.youtube.com/watch?v=theBCIC0Pd0
	
	
	
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



----------------------------------------------------------------------

create or replace storage integration s3_int_pipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::025066273338:role/snow-orders-data'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snow-ordres-data/csv/')
   COMMENT = 'This is to practice pipe concept'


   // See storage integration properties to fetch external_id so we can update it in S3

   DESC integration s3_int_pipe;



   // Create table first

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));


// Create file format object

CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;


// Create stage object with integration object & file format object

CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = ' s3://snow-ordres-data/csv/'
    STORAGE_INTEGRATION = s3_int_pipe
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat


    // Create stage object with integration object & file format object

 LIST @MANAGE_DB.external_stages.csv_folder 



 // Create schema to keep things organized

 CREATE OR REPLACE SCHEMA MANAGE_DB.pipes


// Define pipe

CREATE OR REPLACE pipe MANAGE_DB.pipes.orders_pipe
auto_ingest = TRUE
AS
COPY INTO OUR_FIRST_DB.PUBLIC.orders
FROM @MANAGE_DB.external_stages.csv_folder 


// Describe pipe

DESC pipe orders_pipe


SELECT * FROM OUR_FIRST_DB.PUBLIC.orders    

   
