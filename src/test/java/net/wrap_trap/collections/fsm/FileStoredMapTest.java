package net.wrap_trap.collections.fsm;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.wrap_trap.collections.fsm.Configuration;
import net.wrap_trap.collections.fsm.FileStoredMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoredMapTest {

    protected static Logger logger = LoggerFactory.getLogger(FileStoredMapTest.class);

    @Before
    public void setUp() throws IOException {}

    @Test
    public void testPutString() throws IOException {
        TestUtils.deleteFiles("tmp/pridir");
        FileStoredMap<String> map = null;
        try {
            map = new FileStoredMap<String>("tmp/pridir");
            map.put("foo", "bar");
            String ret = map.get("foo");
            assertThat(ret, is("bar"));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testPutInt() throws IOException {
        TestUtils.deleteFiles("tmp/pridir");
        FileStoredMap<Integer> map = null;
        try {
            map = new FileStoredMap<Integer>("tmp/pridir");
            map.put("foo", 1);
            int ret = map.get("foo");
            assertThat(ret, is(1));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testPutPojo() throws IOException {
        TestUtils.deleteFiles("tmp/empdir");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/empdir");
            Date createdAt = new Date();
            Employee emp = TestUtils.createEmployee("hoge", 256, createdAt);
            map.put("emp", emp);
            TestUtils.assertEmployeeEquivalent(emp, map.get("emp"));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testPutNestedPojo() throws IOException {
        TestUtils.deleteFiles("tmp/empdir2");
        long start = System.currentTimeMillis();
        FileStoredMap<Employer> map = null;
        try {
            map = new FileStoredMap<Employer>("tmp/empdir2");
            Employee emp1 = TestUtils.createEmployee("foo", 256, new Date());
            Employee emp2 = TestUtils.createEmployee("bar", 65536, new Date());
            Employer employer = TestUtils.createEmployer("boss");
            employer.addEmployee(emp1);
            employer.addEmployee(emp2);
            long c1 = System.currentTimeMillis();
            logger.debug("c1 - start: {}", c1 - start);

            map.put("emp", employer);
            long c2 = System.currentTimeMillis();
            logger.debug("c2 - c1: {}", c2 - c1);

            Employer rEmployer = map.get("emp");
            long c3 = System.currentTimeMillis();
            logger.debug("c3 - c2: {}", c3 - c2);

            assertThat(rEmployer.getName(), is(employer.getName()));
            assertThat(rEmployer.getEmpList().size(), is(2));

            TestUtils.assertEmployeeEquivalent(emp1, rEmployer.getEmpList().get(0));
            TestUtils.assertEmployeeEquivalent(emp2, rEmployer.getEmpList().get(1));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testGetNull() throws IOException {
        TestUtils.deleteFiles("tmp/remove1");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/remove1");
            Employee emp = TestUtils.createEmployee("hoge", 1, new Date());
            map.put("emp", emp);
            assertThat(map.get("emp2"), nullValue());
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testRemoveNoIndexFile() throws IOException {
        TestUtils.deleteFiles("tmp/remove2");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/remove2");
            assertThat(map.get("emp2"), nullValue());
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testRemoveNull() throws IOException {
        TestUtils.deleteFiles("tmp/remove3");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/remove3");
            Employee emp = TestUtils.createEmployee("hoge", 256, new Date());
            map.put("emp", emp);
            assertThat(map.remove("emp2"), nullValue());
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testRemove() throws IOException {
        TestUtils.deleteFiles("tmp/remove4");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/remove4");
            Date createdAt = new Date();
            Employee emp = TestUtils.createEmployee("hoge", 256, createdAt);
            map.put("emp", emp);
            TestUtils.assertEmployeeEquivalent(emp, map.remove("emp"));
            assertThat(map.get("emp"), nullValue());
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testReopen() throws IOException {
        TestUtils.deleteFiles("tmp/reopen");
        FileStoredMap<Employee> map = null;
        Employee emp1 = null;
        try {
            map = new FileStoredMap<Employee>("tmp/reopen");
            emp1 = TestUtils.createEmployee("foo", 256, new Date());
            map.put("emp1", emp1);
            Employee ret = map.get("emp1");
            TestUtils.assertEmployeeEquivalent(emp1, ret);
        } finally {
            if (map != null) {
                map.close();
            }
        }

        try {
            map = new FileStoredMap<Employee>(TestUtils.createConfiguration("tmp/reopen", 512));
            Employee ret = map.get("emp1");
            TestUtils.assertEmployeeEquivalent(emp1, ret);
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testClear() throws IOException {
        TestUtils.deleteFiles("tmp/clear");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/clear");
            Employee emp1 = TestUtils.createEmployee("foo", 256, new Date());
            map.put("emp1", emp1);
            TestUtils.assertEmployeeEquivalent(emp1, map.get("emp1"));
            assertThat(map.size(), is(1));
            map.clear();
            assertThat(map.get("emp1"), nullValue());
            assertThat(map.size(), is(0));
            map.put("emp1", emp1);
            TestUtils.assertEmployeeEquivalent(emp1, map.get("emp1"));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testKeySet() throws IOException {
        TestUtils.deleteFiles("tmp/keyset");
        FileStoredMap<Employee> map = null;
        Set<String> expected = new HashSet<String>();
        expected.add("emp1");
        expected.add("emp2");
        expected.add("emp3");
        try {
            map = new FileStoredMap<Employee>(TestUtils.createConfiguration("tmp/keyset", 2));
            map.put("emp1", TestUtils.createEmployee("hoge", 256, new Date()));
            map.put("emp2", TestUtils.createEmployee("foo", 128, new Date()));
            map.put("emp3", TestUtils.createEmployee("bar", 64, new Date()));

            for (String key : map.keySet()) {
                assertThat(expected.remove(key), is(true));
            }
            assertThat(expected.isEmpty(), is(true));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testContainsKey() throws IOException {
        TestUtils.deleteFiles("tmp/testContainsKey");
        FileStoredMap<Employee> map = null;
        Set<String> expected = new HashSet<String>();
        expected.add("emp1");
        expected.add("emp2");
        expected.add("emp3");
        try {
            map = new FileStoredMap<Employee>(TestUtils.createConfiguration("tmp/testContainsKey", 2));
            map.put("emp1", TestUtils.createEmployee("hoge", 256, new Date()));
            map.put("emp2", TestUtils.createEmployee("foo", 128, new Date()));
            map.put("emp3", TestUtils.createEmployee("bar", 64, new Date()));

            for (String key : expected) {
                assertThat(map.containsKey(key), is(true));
                assertThat(map.remove(key), is(notNullValue()));
            }
            assertThat(map.isEmpty(), is(true));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testPutAll() throws IOException {
        TestUtils.deleteFiles("tmp/testPutAll");
        FileStoredMap<Employee> map = null;
        Set<String> expected = new HashSet<String>();
        expected.add("emp1");
        expected.add("emp2");
        expected.add("emp3");

        Map<String, Employee> container = new HashMap<String, Employee>();
        container.put("emp1", TestUtils.createEmployee("hoge", 256, new Date()));
        container.put("emp2", TestUtils.createEmployee("foo", 128, new Date()));

        try {
            map = new FileStoredMap<Employee>(TestUtils.createConfiguration("tmp/testPutAll", 2));
            map.put("emp3", TestUtils.createEmployee("bar", 64, new Date()));
            map.putAll(container);
            for (String key : expected) {
                assertThat(map.containsKey(key), is(true));
                assertThat(map.remove(key), is(notNullValue()));
            }
            assertThat(map.isEmpty(), is(true));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testValues() throws IOException {
        TestUtils.deleteFiles("tmp/values");
        FileStoredMap<Employee> map = null;

        Employee emp1 = TestUtils.createEmployee("hoge", 256, new Date());
        Employee emp2 = TestUtils.createEmployee("foo", 128, new Date());
        Employee emp3 = TestUtils.createEmployee("bar", 64, new Date());

        Set<Employee> expected = new HashSet<Employee>();
        expected.add(emp1);
        expected.add(emp2);
        expected.add(emp3);
        try {
            map = new FileStoredMap<Employee>(TestUtils.createConfiguration("tmp/values", 2));
            map.put("emp1", emp1);
            map.put("emp2", emp2);
            map.put("emp3", emp3);

            for (Employee emp : map.values()) {
                boolean found = false;
                for (Employee target : expected) {
                    if (target.getName().equals(emp.getName())) {
                        TestUtils.assertEmployeeEquivalent(target, emp);
                        found = true;
                    }
                }
                if (!found) {
                    Assert.fail(String.format("%s was not found in expected set.", emp.getName()));
                }
            }
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testDataFileSizeThreshold() throws IOException {
        TestUtils.deleteFiles("tmp/datafilesize");
        FileStoredMap<String> map = null;
        try {
            Configuration configuration = new Configuration();
            configuration.setDirPath("tmp/datafilesize");
            configuration.setDataFileSize(32);
            map = new FileStoredMap<String>(configuration);
            map.put("a", "aaaaa");
            File file = new File("tmp/datafilesize");
            FilenameFilter filter = new DataFilenameFilter();
            String[] dataFiles = file.list(filter);
            assertThat(dataFiles.length, is(1));

            map.put("b", "aaaaaa");
            dataFiles = file.list(filter);
            assertThat(dataFiles.length, is(2));

            assertThat("aaaaaa", is(map.get("b")));
            assertThat("aaaaa", is(map.get("a")));
        } finally {
            if (map != null) {
                map.close();
            }
        }

    }

    class DataFilenameFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".dat");
        }

    }
}
