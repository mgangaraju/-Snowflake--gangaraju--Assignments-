nowflake Realtime Training 
								        Session-5
								--------------------------
								
								Advanced SQL concepts
								
								Rownumber  
								rank
								dense_rank
								CTE 
								Union 
								Union All   
								
								
								
	Assignment :  identify the person joined early into my orgnizationn based on job id  
	
	vitschool21@gmail.com
	vivisionschool21@gmail.com

class notes:
-----------------
select department_id from employees



select * from employees


select 
       row_number() over(order by department_id) as rid,
       department_id,
       salary,
       first_name
from employees


select * from (
select 
       row_number() over(order by department_id) as rid,
       department_id,
       salary,
       first_name
from employees
) where rid = 10

create or replace table emp as (
select 
       row_number() over(order by department_id) as rid,
       department_id,
       salary,
       first_name
from employees
)
select * from emp where rid = 10

--CTE -- common table expression 
with cte_emp as (
select 
       row_number() over(order by department_id) as rid,
       department_id,
       salary,
       first_name
from employees 
) 
select * from cte_emp where rid = 10

--------------------------
with cte_emp as (
select 
       row_number() over(order by salary desc) as rid,
       salary,
       first_name,
       department_id,
from employees 
) 
select * from cte_emp where rid = 6
