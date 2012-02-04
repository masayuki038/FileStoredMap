package net.wrap_trap.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Employer implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -5314892812089871791L;

    private String name;
    private List<Employee> empList = new ArrayList<Employee>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Employee> getEmpList() {
        return empList;
    }

    public void setEmpList(List<Employee> empList) {
        this.empList = empList;
    }

    public void addEmployee(Employee emp) {
        empList.add(emp);
    }
}
