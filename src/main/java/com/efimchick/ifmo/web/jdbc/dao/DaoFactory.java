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
import java.util.Optional;


public class DaoFactory {
        private ResultSet getResultset(String sql){
            try {
                ConnectionSource source = ConnectionSource.instance();
                Connection connection = source.createConnection();
                return connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }

        private void  Refresh(String sql){
            try {
                ConnectionSource source = ConnectionSource.instance();
                Connection connection = source.createConnection();
                connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeUpdate(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


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

    private List<Employee> getEmployees(ResultSet resultset) throws SQLException{
        List<Employee> allEmployees = new ArrayList<>();
        while (resultset.next()){
            Employee emp = EmployeeRowMapper(resultset);
            allEmployees.add(emp);
        }
        return allEmployees;
    }

    private List<Department> getDepartment(ResultSet resultset) throws SQLException{
        List<Department> allDepartments = new ArrayList<>();
        resultset.absolute(0);
        while (resultset.next()){
            Department dep = new Department(new BigInteger(resultset.getString("ID")),
                    resultset.getString("NAME"), resultset.getString("LOCATION"));
            allDepartments.add(dep);
        }
        return allDepartments;
    }





    public EmployeeDao employeeDAO(){

        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                ResultSet resultset = getResultset("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId());
                assert resultset != null;
                try {
                    return getEmployees(resultset);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }

            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                ResultSet resultset = getResultset("SELECT * FROM EMPLOYEE WHERE MANAGER = " + employee.getId());
                assert resultset != null;
                try {
                    return getEmployees(resultset);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }

            }

            @Override
            public Optional<Employee> getById(BigInteger Id){
                ResultSet resultset = getResultset("SELECT * FROM EMPLOYEE WHERE ID = " + Id);
                assert resultset != null;
                try {
                    if (resultset.next()){
                        return Optional.ofNullable(EmployeeRowMapper(resultset));
                    }
                    else return Optional.empty();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                ResultSet resultset = getResultset("SELECT * FROM EMPLOYEE");
                assert resultset != null;
                try {
                    return getEmployees(resultset);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                Refresh("INSERT INTO employee VALUES" +
                        "('" + employee.getId() +
                        "','" + employee.getFullName().getFirstName() +
                        "','" + employee.getFullName().getLastName() +
                        "','" + employee.getFullName().getMiddleName() +
                        "','" + employee.getPosition() +
                        "','" + employee.getManagerId() +
                        "','" + employee.getHired() +
                        "','" + employee.getSalary() +
                        "','" + employee.getDepartmentId() +
                        "')"
                );
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                Refresh("DELETE FROM employee WHERE ID = " + employee.getId());
            }
        };
    }

    public DepartmentDao departmentDAO()  {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try {
                    ResultSet resultset = getResultset("SELECT * FROM DEPARTMENT WHERE ID = " + Id);
                    //System.out.println(getDepartment(resultset).get(0));
                    if (resultset.next()){
                        return Optional.of(getDepartment(resultset).get(0));
                    }
                    else{
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                ResultSet resultset = getResultset("SELECT * FROM DEPARTMENT");
                assert resultset != null;
                try {
                    return getDepartment(resultset);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                ResultSet resultset = getResultset("SELECT * FROM DEPARTMENT WHERE ID = " + department.getId());
                try {
                    assert resultset != null;
                    if (resultset.next()) {
                        Refresh("UPDATE department SET NAME = " +
                                "'" + department.getName() +
                                "', LOCATION = '" + department.getLocation() +
                                "' WHERE ID = '" + department.getId() + "'");
                    } else {
                        Refresh("INSERT INTO department VALUES" +
                                "('" + department.getId() +
                                "','" + department.getName() +
                                "','" + department.getLocation() + "')");
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                Refresh("DELETE FROM department WHERE ID = " + department.getId());
            }
        };
    }
}

