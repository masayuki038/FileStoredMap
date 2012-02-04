package net.wrap_trap.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SomethingCallee.class)
public class HashCollisionTest {

    @Test
    public void testHashCodeEquivalent() {
        String foo = "foo";
        String bar = "bar";

        assertThat(foo.hashCode(), is(not(bar.hashCode())));

        bar = spy(bar);
        when(bar.hashCode()).thenReturn(foo.hashCode());

        assertThat(bar, is("bar"));
        assertThat(foo.hashCode(), is(bar.hashCode()));
        SomethingCallee.checkHashcodeEquivalent(foo, bar);
    }

    public void testSpyWithPojo() {
        Employee emp1 = TestUtils.createEmployee("name1", 1, new Date());
        emp1 = spy(emp1);
        when(emp1.getName()).thenReturn("name2");

        assertThat(emp1.getName(), is("name2"));
        SomethingCallee.checkEmployeeNameEquivalent(emp1, "name2");
    }
}
