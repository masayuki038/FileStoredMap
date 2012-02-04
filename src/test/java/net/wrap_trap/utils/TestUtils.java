package net.wrap_trap.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;

public class TestUtils {
    public static Employee createEmployee(String name, int sal, Date created) {
        Employee employee = new Employee();
        employee.setName(name);
        employee.setSal(sal);
        employee.setCreatedAt(created);
        return employee;
    }

    public static Employer createEmployer(String name) {
        Employer employer = new Employer();
        employer.setName(name);
        return employer;
    }

    public static void deleteFiles(String path) {
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException ignore) {}
    }

    public static void assertEmployeeEquivalent(Employee a, Employee b) {
        assertThat(a.getName(), is(b.getName()));
        assertThat(a.getSal(), is(b.getSal()));
        assertThat(a.getCreatedAt(), is(b.getCreatedAt()));
    }

}
