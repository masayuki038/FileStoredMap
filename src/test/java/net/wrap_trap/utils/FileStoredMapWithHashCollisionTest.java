package net.wrap_trap.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileStoredMap.class)
public class FileStoredMapWithHashCollisionTest {
    @Test
    public void testGetWithHashCollistion() throws IOException {
        String key1 = "key1";
        String key2 = "key2";

        key2 = spy(key2);
        when(key2.hashCode()).thenReturn(key1.hashCode());
        assertThat(key1.hashCode(), is(key2.hashCode()));

        TestUtils.deleteFiles("tmp/collistion1");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/collistion1");
            Employee emp1 = TestUtils.createEmployee("name1", 1, new Date());
            map.put(key1, emp1);
            Employee emp2 = TestUtils.createEmployee("name2", 2, new Date());
            map.put(key2, emp2);

            TestUtils.assertEmployeeEquivalent(emp1, map.get(key1));
            TestUtils.assertEmployeeEquivalent(emp2, map.get(key2));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

    @Test
    public void testRemoveWithHashCollistion() throws IOException {
        String key1 = "key1";
        String key2 = "key2";

        key2 = spy(key2);
        when(key2.hashCode()).thenReturn(key1.hashCode());
        assertThat(key1.hashCode(), is(key2.hashCode()));

        TestUtils.deleteFiles("tmp/collistion2");
        FileStoredMap<Employee> map = null;
        try {
            // remove key1 -> key2
            map = new FileStoredMap<Employee>("tmp/collistion2");
            Employee emp1 = TestUtils.createEmployee("name1", 1, new Date());
            map.put(key1, emp1);
            Employee emp2 = TestUtils.createEmployee("name2", 2, new Date());
            map.put(key2, emp2);

            TestUtils.assertEmployeeEquivalent(emp1, map.remove(key1));
            assertThat(map.get(key1), nullValue());

            TestUtils.assertEmployeeEquivalent(emp2, map.remove(key2));
            assertThat(map.get(key2), nullValue());

            // remove key2 -> key1
            map.put(key1, emp1);
            map.put(key2, emp2);

            TestUtils.assertEmployeeEquivalent(emp2, map.remove(key2));
            assertThat(map.get(key2), nullValue());

            TestUtils.assertEmployeeEquivalent(emp1, map.remove(key1));
            assertThat(map.get(key1), nullValue());
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

}
