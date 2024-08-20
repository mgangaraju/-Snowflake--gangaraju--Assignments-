show tables;

select * from ORDERS ;


create table ORDERS_clone  
clone ORDERS 


delete from ORDERS ;

select * from ORDERS ;

//Cloning 
    SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS   

    CREATE TABLE OUR_FIRST_DB.PUBLIC.ORDERS_CLONE1 
    CLONE  OUR_FIRST_DB.PUBLIC.ORDERS   

//Validate the Data 
SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_CLONE1   

SHOW TABLES IN OUR_FIRST_DB.PUBLIC;

//Update Cloned table
UPDATE OUR_FIRST_DB.PUBLIC.ORDERS_CLONE1  
SET PROFIT = null

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS_CLONE1
 SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS  

 

SHOW TABLES IN OUR_FIRST_DB.PUBLIC;


//Cloning a temporary table is not possible 
CREATE OR REPLACE TEMPORARY TABLE  OUR_FIRST_DB.PUBLIC.TEMP_TABLE (id INT)

CREATE TABLE OUR_FIRST_DB.PUBLIC.TEMP_TABLE_copy
CLONE OUR_FIRST_DB.PUBLIC.TEMP_TABLE

//Temp table cannot be cloned to a permanent table; clone to a transient table instead.
//But we can clone temporary to temporary 


// Cloning Schema
CREATE TRANSIENT SCHEMA OUR_FIRST_DB.COPIED_SCHEMA
CLONE OUR_FIRST_DB.PUBLIC;

SELECT * FROM COPIED_SCHEMA.ORDERS


CREATE TRANSIENT SCHEMA OUR_FIRST_DB.EXTERNAL_STAGES_COPIED
CLONE MANAGE_DB.EXTERNAL_STAGES;



// Cloning Database
CREATE TRANSIENT DATABASE OUR_FIRST_DB_COPY
CLONE OUR_FIRST_DB;

DROP DATABASE OUR_FIRST_DB_COPY
DROP SCHEMA OUR_FIRST_DB.EXTERNAL_STAGES_COPIED
DROP SCHEMA OUR_FIRST_DB.COPIED_SCHEMA



SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS ;  --28


DELETE  FROM OUR_FIRST_DB.PUBLIC.ORDERS WHERE  QUANTITY >= 2


SELECT * FROM OUR_FIRST_DB.COPIED_SCHEMA.ORDERS ; -- 285


ALTER TABLE OUR_FIRST_DB.COPIED_SCHEMA.ORDERS 
 SWAP WITH OUR_FIRST_DB.PUBLIC.ORDERS  
