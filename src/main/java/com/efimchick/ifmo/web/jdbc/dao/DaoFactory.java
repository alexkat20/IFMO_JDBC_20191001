package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class DaoFactory {
        private Employee EmployeeRowMapper(ResultSet resultset){
            try{

                return  new Employee(new BigInteger(resultset.getString("ID")),
                        new FullName(resultset.getString("FIRSTNAME"), resultset.getString("LASTNAME"),
                                resultset.getString("MIDDLENAME")),
                        Position.valueOf(resultset.getString("POSITION")),
                        LocalDate.parse(resultset.getString("HIREDATE")),
                        new BigDecimal(resultset.getString("SALARY")),
                        BigInteger.valueOf(resultset.getInt("MANAGER")),
                        BigInteger.valueOf(resultset.getInt("department")));


            } catch (SQLException e) {
                return null;
            }
    }

    private List<Employee> getEmployees() throws SQLException{
        List<Employee> allEmployees = new ArrayList<>();

        Connection connection = ConnectionSource.createConnection();
        ResultSet resultset = connection.createStatement().executeQuery("select * from Employee");
        while (resultset.next()){
            Employee emp = EmployeeRowMapper(resultset);
            allEmployees.add(emp);
        }
        return allEmployees;
    }

    private List<Department> getDepartment() throws SQLException{
        List<Department> allDepartments = new ArrayList<>();
        Connection connection = ConnectionSource.createConnection();
        ResultSet resultset = connection.createStatement().executeQuery("select * from DEPARTMENT");
        while (resultset.next()){
            Department dep = new Department(new BigInteger(resultset.getString("ID")),
                    resultset.getString("NAME"), resultset.getString("LOCATION"));
            allDepartments.add(dep);
        }
        return allDepartments;
    }





    public EmployeeDao employeeDAO() throws SQLException{
        List<Employee> employees = getEmployees();

        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                List<Employee> deps = new ArrayList<>();
                for(Employee emp : employees){

                    if (Objects.equals(emp.getDepartmentId(), department.getId())){
                        deps.add(emp);
                    }
                }
                return deps;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> managers = new ArrayList<>();
                for (Employee emp : employees){
                    if(Objects.equals(emp.getManagerId(), employee.getId())){
                        managers.add(emp);
                    }
                }

                return managers;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                for (Employee emp : employees){
                    if (Objects.equals(emp.getId(), Id)){
                        return Optional.of(emp);
                    }
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                return employees;
            }

            @Override
            public Employee save(Employee employee) {
                if (employee != null){
                    for (Employee emp : employees){
                        if (Objects.equals(emp.getId(), employee.getId())){
                            employees.remove(emp);
                        }
                    }
                    employees.add(employee);
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                if (employee != null){
                    employees.remove(employee);
                }
            }
        };
    }

    public DepartmentDao departmentDAO() throws SQLException {
        List<Department> departments = getDepartment();
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                for (Department dep : departments){
                    if (Objects.equals(dep.getId(), Id)){
                        return Optional.of(dep);
                    }
                }
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                return departments;
            }

            @Override
            public Department save(Department department) {
                if (department != null){
                    for (Department dep : departments){
                        if (Objects.equals(department.getId(), dep.getId())){
                            departments.remove(dep);
                        }
                    }
                    departments.add(department);
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                departments.remove(department);
            }
        };
    }
}
