package com.efimchick.ifmo.web.jdbc;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.util.Set;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;


class SetMapperFactory {

    SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            Set<Employee> Employees = new HashSet<>();
            try{
                while (resultSet.next()){
                    Employee emp = mapRow(resultSet);
                    Employees.add(emp);
                }
            }
            catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
            return Employees;
        };

        //throw new UnsupportedOperationException();
    }

    private Employee mapRow(ResultSet resultSet) {
        try{
            Employee manager = Manager(resultSet);

            return new Employee(new BigInteger(resultSet.getString("ID")),
              new FullName(resultSet.getString("FIRSTNAME"), resultSet.getString("LASTNAME"), resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    new BigDecimal(resultSet.getString("SALARY")), manager);


        }
        catch (SQLException e) {
            return null;
        }

    }

    private Employee Manager(ResultSet resultSet) throws SQLException {
        Employee manager = null;
        int now = resultSet.getRow();
        if (resultSet.getObject("MANAGER") != null){
            String id = resultSet.getString("MANAGER");


            resultSet.absolute(0);
            //manager = mapRow(resultSet);

            if (resultSet.next() && !resultSet.getString("ID").equals(id)) {
                do {
                }
                while (resultSet.next() && !resultSet.getString("ID").equals(id));
            }
            manager = mapRow(resultSet);
        }
        resultSet.absolute(now);

        //System.out.println(id);
        return manager;
    }

}
