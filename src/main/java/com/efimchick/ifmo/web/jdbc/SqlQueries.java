package com.efimchick.ifmo.web.jdbc;



/**
 * Implement sql queries like described
 */
public class SqlQueries {
    //Select all employees sorted by last name in ascending order
    //language=HSQLDB
    String select01 = "SELECT * FROM employee  order by lastname";

    //Select employees having no more than 5 characters in last name sorted by last name in ascending order
    //language=HSQLDB
    String select02 = "SELECT * FROM employee where length(lastname) <= 5 order by lastname";

    //Select employees having salary no less than 2000 and no more than 3000
    //language=HSQLDB
    String select03 = "SELECT * from employee where salary between 2000 and 3000";

    //Select employees having salary no more than 2000 or no less than 3000
    //language=HSQLDB
    String select04 = "SELECT * from employee where salary <= 2000 or salary >= 3000";

    //Select employees assigned to a department and corresponding department name
    //language=HSQLDB
    String select05 = "select employee.id, employee.firstname, employee.lastname, employee.salary, department.name from employee, department where employee.department = department.id";

    //Select all employees and corresponding department name if there is one.
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select06 = "select employee.id, employee.firstname, employee.lastname, employee.salary, department.name as depname from employee left join department on employee.department = department.id   ";

    //Select total salary pf all employees. Name it "total".
    //language=HSQLDB
    String select07 = "select sum(salary) as total from employee";

    //Select all departments and amount of employees assigned per department
    //Name column containing name of the department "depname".
    //Name column containing employee amount "staff_size".
    //language=HSQLDB
    String select08 = "select department.name as depname, count(employee.firstname) as staff_size from employee, department where employee.department = department.id group by department.name";

    //Select all departments and values of total and average salary per department
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select09 = "select  department.name as depname, sum(employee.salary) as total, avg(employee.salary) as average from employee, department where employee.department = department.id group by department.name";

    //Select all employees and their managers if there is one.
    //Name column containing employee lastname "employee".
    //Name column containing manager lastname "manager".
    //language=HSQLDB
    String select10 = "select emp1.lastname as employee, emp2.lastname as manager from employee emp1 left join employee emp2 on emp2.id = emp1.manager";




}
