create table test(amount int);

insert into  test(amount) values(-100),(100),(200),(300),(-10),(-20),(400),(-50);

select * from test;

select amount, 
case when amount >0 then 'profit' else 'loss' end  as status from test;


select * from employees;

update employees 
     set first_name = 'Gangaraju'
     where employee_id = 101;


update employees 
     set first_name = 'Sreekanth'
        ,last_name = 'G'
    where employee_id = 100;

select * from employees where  manager_id = 101;

update employees set manager_id = null
 where manager_id = 100 ;

 update employees 
     set manager_id =  100 
    where manager_id is null ; 


select * from employee_v1 ;

create table employee_v1 as (
select * from employees
);


create table employee_v2 as (
select employee_id ,first_name , salary,phone_number from employees);


select * from employee_v2;
    

select * from dependents;

select top 3 * from dependents;

select  employee_id from dependents;

select  first_name as "FName" from dependents;

select count(*) as "TotalRows" from dependents;


select * from employees  where employee_id in (
   select  employee_id from dependents);

select * from employees where employee_id not in (select employee_id from dependents);


select employee_id ,
       first_name,
       salary ,
       case when salary < 10000 then 'Jr Software' else 'Sr Software ' end  as "designation"
      -- CASE when age >= 10  then "eligble "  else "not eligible" end 
       from employees;

   select email from employees;

select upper (email) from employees;     

select lower (email) from employees;

select substr(email,1,6) from employees; 

select current_timestamp();


select substr(current_timestamp(),1,16);

select distinct salary, from employees;



select employee_id,first_name,salary,row_number()
over (order by salary desc  ) as rid from employees;

select employee_id ,first_name,salary,rank() 
over (order by salary desc) as rid from employees;


select employee_id , first_name , salary 
      , dense_rank() over(order by salary desc ) as rid from employees;