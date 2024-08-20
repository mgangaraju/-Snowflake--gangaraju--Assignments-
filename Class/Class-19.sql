
								        Session-19
								--------------------------
								
								Azure integration with snowflake 
								
  path: 	https://vitechcustdata.blob.core.windows.net/vitech-cars-data


   tenat id:    33e91482-094e-448e-a05b-521ef98fc090  
   
								
	app name -->   et59z4snowflakepacint_1723617051764 
	
	https://login.microsoftonline.com/33e91482-094e-448e-a05b-521ef98fc090/oauth2/authorize?client_id=7f97a3fe-0c75-49d6-927d-41d6bce6cb9f&response_type=code
	
	
	
	
	Class Notes:
	--------------------------
	
	1.Create azure account --> portal.azure.com 


2. Create storage container 
     search your storage container 
          --> data storage
            --> Containers 
3. Creating a container 
		-> Select storage container --> Create Container  (snowflakecsv)
		                                                  ( snowfalkejson)
														  
			Load Data -->
				upload json/csv from locally 

4. Creating Integration Object before that find tenat_id 
	
	TO find TENANT_ID ??  search tenant or 
		type azure << active directory (or) Microsoft entra ID >> and find tenant inforamtion and copy
		
		Tenant ID = c20c9531-4384-4115-a44d-b8bf8118a28f
		
		Storage account and navigate to containers and copy the path 



    USE DATABASE DEMO_DB;
	-- create integration object that contains the access information
	CREATE or replace STORAGE INTEGRATION azure_integration
	  TYPE = EXTERNAL_STAGE
	  STORAGE_PROVIDER = AZURE
	  ENABLED = TRUE
	  AZURE_TENANT_ID = '33e91482-094e-448e-a05b-521ef98fc090' 
	  STORAGE_ALLOWED_LOCATIONS = ('azure://vitechempdata.blob.core.windows.net/snowcsv');

	  
5	-- Describe integration object to provide access     
	DESC STORAGE integration azure_integration;
	
	Below are important steps 
	-----------------------------
    AZURE_CONSENT_URL : https://login.microsoftonline.com/5db02a30-c9f2-41a5-a6a2-6946a48461c4/oauth2/authorize?client_id=a28ec89f-086e-41e1-82cf-51e7b3f04faa&response_type=code
	AZURE_MULTI_TENANT_APP_NAME : ybaalisnowflakepacint_1721969803255

	Here need to copy AZURE_CONSENT_URL    
	                    copy url and paste in browser in new tab to grant access (provide consent)
						Still need to give access Add role --> Role Assignments 
		Storage account --> IAM --> role --> Add Role Assignments --> search -->text as (Container)--> (storage blob data contributor)
                    copy app name from integration object and add into members(ybaalisnowflakepacint)
							       Select memeber(ybaalisnowflakepacint) 
                               Click on Save 
           Now storage object has access for containers 


-------------



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
CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_azure_folder
    URL = 'azure://vitechcustdata.blob.core.windows.net/vitech-cars-data/'
    STORAGE_INTEGRATION = azure_integration
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat
   

 // Create stage object with integration object & file format object
LIST @MANAGE_DB.external_stages.csv_azure_folder  


-------------------------------------------------------


// Create storage integration object

create or replace storage integration Azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE 
  AZURE_TENANT_ID = 'eac839df-6ce8-410c-8056-d13e394dbdbb'
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowdataemp1.blob.core.windows.net/snowflakecsv')
   COMMENT = 'This is to practice pipe concept'

-- Describe integration object 

   DESC storage integration Azure_integration


// Create file format object

   CREATE OR REPLACE file format MANAGE_DB.file_formats.Azure
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;


// Create stage object with integration object & file format object

CREATE OR REPLACE stage MANAGE_DB.external_stages.Azure_integration
    URL = ' azure://snowdataemp1.blob.core.windows.net/snowflakecsv'
    STORAGE_INTEGRATION = Azure_integration
    FILE_FORMAT = MANAGE_DB.file_formats.Azure    

 
    // Create stage object with integration object & file format object

 LIST @MANAGE_DB.external_stages.Azure_integration 

 // Create table first

 CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING

  )


  COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.Azure_integration 


SELECT * FROM OUR_FIRST_DB.PUBLIC.employees 

 
   


    