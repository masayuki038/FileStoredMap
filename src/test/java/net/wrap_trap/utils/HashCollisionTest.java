package net.wrap_trap.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.mockito.PowerMockito.*; 

@RunWith(PowerMockRunner.class)
@PrepareForTest(String.class)
public class HashCollisionTest {

	@Test
	public void testHashCodeEquivalent(){
		String foo = "foo";
		String bar = "bar";

		assertThat(foo.hashCode(), is(not(bar.hashCode())));
		
		bar = spy(bar);
		when(bar.hashCode()).thenReturn(foo.hashCode());
		
		assertThat(bar, is("bar"));
		assertThat(foo.hashCode(), is(bar.hashCode()));
	}
}
