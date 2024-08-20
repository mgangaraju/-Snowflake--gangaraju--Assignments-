Session-15
								--------------------------
								 AWS integration  with snowflake 
								 
								 
								 
								 url:   --->  stages --> copy (load data)
								 
1)  Create AWS account 
2)  Create S3 bucket (create folders)
3)  Load the data into bucket   (manual) 
4)  Create IAM Roles   (provide Access)  external id need to keep 0000
5)  Goto snowflake create S3 integration object 
6)  Describe S3 int object and copy User ARN , External id and edit the role trust policy from aws
7)  Create Stages 
8)  List stages 
9)  Load data into table using copy command
10) Create table and file format before 9 th step  



---
url:  s3://vitech-snow11am-batch/csv/
      s3://vitech-snow11am-batch/json/

role arn: arn:aws:iam::024848483653:role/snow-vitech-11am-role



user arn:     arn:aws:iam::211125613752:user/externalstages/ci4zx60000

external id:   RW49251_SFCRole=2_E6kRWCgttF70FVNxmAyHquLbXU4=




TASK :  


// Create storage integration object

create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = ''
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket-name>/<your-path>/', 's3://<your-bucket-name>/<your-path>/')
   COMMENT = 'This an optional comment' 
   
   
// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;





class notes:
---------------------

create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::024848483653:role/snow-vitech-11am-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vitech-snow11am-batch/csv/', 's3://vitech-snow11am-batch/csv/')
   COMMENT = 'This is an s3 integration object ' 



   --------------------------
create database MANAGE_DB;

create schema file_formats;

	DESC integration s3_int;


CREATE OR REPLACE stage stage_aws_csv
    URL = 's3://vitech-snow11am-batch/csv/'
    STORAGE_INTEGRATION = s3_int

list @stage_aws_csv
    
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE    
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  

copy into OUR_FIRST_DB.PUBLIC.movie_titles  
  from @stage_aws_csv 
file_format = (FORMAT_NAME  = MANAGE_DB.file_formats.csv_fileformat)
files =('netflix_titles.csv')

select count(*)  from  OUR_FIRST_DB.PUBLIC.movie_titles;

select *  from  OUR_FIRST_DB.PUBLIC.movie_titles;
    
    -------------------------
create database OUR_FIRST_DB;

// Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.movie_titles (
  show_id STRING,
  type STRING,
  title STRING,
  director STRING,
  cast STRING,
  country STRING,
  date_added STRING,
  release_year STRING,
  rating STRING,
  duration STRING,
  listed_in STRING,
  description STRING )


---------------------------
s3://ganga-snowflake/csv/


role : arn:aws:iam::025066273338:role/snowflake-ganga-role

user arn : arn:aws:iam::211125613752:user/externalstages/ci40y60000
           arn:aws:iam::211125613752:user/externalstages/ci40y60000

EXTERNAL_ID : SX16053_SFCRole=2_+ESJTYiDEjU/x0I4ofhtmzanwb0=

// Create storage integration object

create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::025066273338:role/snowflake-ganga-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ganga-snowflake/csv/', 's3://ganga-snowflake/csv/')
   COMMENT = 'This is an s3 integration object' 


   DESC integration s3_int;

OUR_FIRST_DB.PUBLIC.STAGE_AWS_CSV

CREATE OR REPLACE stage stage_aws_csv
    URL = 's3://ganga-snowflake/csv/'
    STORAGE_INTEGRATION = s3_int;


list @stage_aws_csv  

create schema file_formats;

CREATE OR REPLACE file format OUR_FIRST_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE    
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'


 CREATE OR REPLACE TABLE OUR_FIRST_DB.file_formats.employees (
	employee_id INT ,
	first_name STRING,
	last_name STRING,
	email STRING,
	phone_number STRING,
	hire_date STRING,
    job_title STRING,
	job_id INT,
	salary DECIMAL,
	manager_id INT,
	department_id INT
);   

    copy into OUR_FIRST_DB.file_formats.employees  
  from @stage_aws_csv 
file_format = (FORMAT_NAME  = OUR_FIRST_DB.file_formats.csv_fileformat)
files =('employees.csv')
on_error = 'continue'


select * from OUR_FIRST_DB.file_formats.employees 


