
								        Session-21
								--------------------------
								
								     Table Types 
									 
									
									1) Permanent 
									2) Transient 
									3) Temporary 
									 
									 
	1. Create 3 types of databases 
	2. Create 3 types of tables  
	
	TASK: 							
	https://youtu.be/clOxNvz8apw?si=UpaFGPAvtjmpUnqx	
	
	MySQL To Snowflake (Migration) 
	
	1000  ---> 1000
	
  power bi:	https://www.microsoft.com/en-us/download/details.aspx?id=58494
	
	---------------------------------

    
	CREATE OR REPLACE DATABASE PDB;

CREATE OR REPLACE TABLE PDB.public.customers (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);
  
CREATE OR REPLACE TABLE PDB.public.helper (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);
    
// Stage and file format
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    
LIST  @MANAGE_DB.external_stages.time_travel_stage;


// Copy data and insert in table
COPY INTO PDB.public.helper_tEMP
FROM @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

CREATE OR REPLACE TRANSIENT TABLE PDB.public.helper_TRANS (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);
    
CREATE OR REPLACE TEMPORARY TABLE PDB.public.helper_tEMP (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);

SELECT * FROM PDB.public.helper; --0-90

SELECT * FROM PDB.PUBLIC.helper_tEMP  -- 0-1 

SELECT * FROM PDB.PUBLIC.helper_TRANS --0-1

delete from PDB.public.helper where id >=100

INSERT INTO PDB.public.customers
SELECT
t1.ID
,t1.FIRST_NAME	
,t1.LAST_NAME	
,t1.EMAIL	
,t1.GENDER	
,t1.JOB
,t1.PHONE
 FROM PDB.public.helper t1
CROSS JOIN (SELECT * FROM PDB.public.helper) t2
CROSS JOIN (SELECT TOP 100 * FROM PDB.public.helper) t3;




// Show table and validate
SHOW TABLES;


// Permanent tables

USE OUR_FIRST_DB

CREATE OR REPLACE TABLE customers (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);
  
CREATE OR REPLACE DATABASE PDB;

SHOW DATABASES;

SHOW TABLES;