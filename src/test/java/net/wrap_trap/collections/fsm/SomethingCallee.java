package net.wrap_trap.collections.fsm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SomethingCallee.class)
public class SomethingCallee {

    public static void checkHashcodeEquivalent(String key1, String key2) {
        assertThat(key1.hashCode(), is(key2.hashCode()));
    }

    public static void checkEmployeeNameEquivalent(Employee emp1, String target) {
        assertThat(emp1.getName(), is(target));
    }
}
