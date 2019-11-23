package com.efimchick.ifmo.web.jdbc.service;

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

public class ServiceFactory {

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


    private List<Department> getDepartment() {
        ResultSet resultset = getResultset("SELECT * FROM DEPARTMENT");
        List<Department> allDepartments = new ArrayList<>();
        while (true){
            try {
                assert resultset != null;
                if (!resultset.next()) break;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            Department dep = null;
            try {
                dep = new Department(new BigInteger(resultset.getString("ID")),
                        resultset.getString("NAME"), resultset.getString("LOCATION"));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            allDepartments.add(dep);
        }
        return allDepartments;
    }




    private Employee EmployeeRowMapper(ResultSet resultset, boolean c, boolean Mainmanager){

        try{
            Employee manager = null;
            BigInteger managerId = BigInteger.valueOf(resultset.getInt("MANAGER"));
            BigInteger depId = BigInteger.valueOf(resultset.getInt("department"));
            List<Department> departments = getDepartment();
            Department result = null;
            for (Department department : departments) {
                if (department.getId().equals(depId)) {
                    result = department;
                }
            }

            if (managerId != null && Mainmanager ) {
                if (!c) Mainmanager = false;

                ResultSet ResultSet = getResultset("SELECT * FROM EMPLOYEE");
                assert ResultSet != null;
                while (ResultSet.next()) {
                    if (BigInteger.valueOf(ResultSet.getInt("ID")).equals(managerId)) {
                        manager = EmployeeRowMapper(ResultSet, c, Mainmanager);
                    }
                }
            }
            return new Employee(new BigInteger(resultset.getString("ID")),
                    new FullName(resultset.getString("FIRSTNAME"), resultset.getString("LASTNAME"),
                            resultset.getString("MIDDLENAME")),
                    Position.valueOf(resultset.getString("POSITION")),
                    LocalDate.parse(resultset.getString("HIREDATE")),
                    new BigDecimal(resultset.getString("SALARY")),
                    manager,
                    result);

        } catch (SQLException e) {
            return null;
        }
    }

    private List<Employee> getEmployees(ResultSet resultset, boolean c) throws SQLException{
        List<Employee> allEmployees = new ArrayList<>();
        if (resultset != null){
            while (resultset.next()){
            Employee emp = EmployeeRowMapper(resultset, c, true);
            allEmployees.add(emp);
        }
            return allEmployees;
        }
        else return null;
    }

    private List<Employee> employeeList1(ResultSet resultSet) {
        try {
            return getEmployees(resultSet, true);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    private List<Employee> employeeList2(ResultSet resultSet) {
        try {
            return getEmployees(resultSet, false);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    public EmployeeService employeeService(){



        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE ORDER BY HIREDATE limit " + paging.itemPerPage +
                        " offset " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE ORDER BY LASTNAME LIMIT " + paging.itemPerPage +
                        " offset " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE ORDER BY SALARY LIMIT " + paging.itemPerPage +
                        " offset " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE LEFT OUTER JOIN DEPARTMENT D1 ON EMPLOYEE.DEPARTMENT = D1.ID ORDER BY D1.NAME, EMPLOYEE.LASTNAME" +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE LEFT OUTER JOIN DEPARTMENT D1 ON EMPLOYEE.DEPARTMENT = D1.ID WHERE D1.ID = " + department.getId() +
                        " ORDER BY HIREDATE " +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE LEFT OUTER JOIN DEPARTMENT D1 ON EMPLOYEE.DEPARTMENT = D1.ID WHERE D1.ID = " + department.getId() +
                        " ORDER BY SALARY " +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE LEFT OUTER JOIN DEPARTMENT D1 ON EMPLOYEE.DEPARTMENT = D1.ID WHERE D1.ID = " + department.getId() +
                        " ORDER BY LASTNAME " +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId() +
                        " ORDER BY LASTNAME " +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId() +
                        " ORDER BY HIREDATE" +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE WHERE MANAGER = " + manager.getId() +
                        " ORDER BY SALARY" +
                        " LIMIT " + paging.itemPerPage +
                        " OFFSET " + (paging.itemPerPage * (paging.page - 1)));
                return employeeList2(resultSet);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE WHERE ID = " + employee.getId());
                return Objects.requireNonNull(employeeList1(resultSet)).get(0);
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                ResultSet resultSet = getResultset("SELECT * FROM EMPLOYEE LEFT OUTER JOIN DEPARTMENT D1 ON EMPLOYEE.DEPARTMENT = D1.ID WHERE D1.ID = " + department.getId() +
                        " ORDER BY SALARY DESC " +
                        " LIMIT " + 1 +
                        " OFFSET " + (salaryRank - 1));
                return Objects.requireNonNull(employeeList2(resultSet)).get(0);
            }
        };
    }
}
