select * from employees; 

show tables;

select * from EMPLOYEES where salary >= 10000;


select * from employees;

select   EMPLOYEE_ID,
         FIRST_NAME,
         EMAIL,
         salary from employees;
         
              
select * from employees where DEPARTMENT_ID = 8 ; 

select employee_id,first_name,email,salary from employees where employee_id =201 
                                                             or employee_id =202
                                                             or employee_id =203;


  select employee_id ,first_name ,salary,email
         from employees where employee_id in (201,202,203,205);


select * from employees where first_name like 'M%'
                                        and salary >=10000  ;                                                       
select * from employees where manager_id is null;
select * from employees where manager_id is not null;

select count(*) from employees;

select count(first_name) from employees;

select count(manager_id) from employees;

select employee_id ,
      first_name ,
      salary ,
      email
      from employees order by salary ;

select employee_id ,
      first_name ,
      salary ,
      email
      from employees order by salary desc;      

 select employee_id ,
      first_name ,
      salary ,
      email
      from employees order by first_name  ;

 select employee_id ,
      first_name ,
      salary ,
      email
      from employees order by first_name desc;  



select * from employees where salary between 15000 and 25000 order by salary ;   


select DEPARTMENT_ID from employees;

select count(*) from employees where DEPARTMENT_ID = 10;

select distinct(department_id) from employees order by 1; 

select department_id, count(*) from employees group by department_id ;

select department_id from employees group by department_id;


select first_name from employees;



select distinct(first_name) from employees;

select first_name ,count(*) from employees group by first_name;

select first_name, count(*) from employees group by  first_name having count(*)>1;


select salary from employees;

select max(salary) from employees;
select min (salary) from employees;
select sum(salary) from employees;

select * from employees where salary in (select max (salary)from employees);

select department_id, sum(salary) from employees group by department_id;

select max(salary) from employees  where salary not in (select max(salary) from employees )


 