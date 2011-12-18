package net.wrap_trap.utils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class FileStoredMapTest {
	
	@Before
	public void setUp() throws IOException{
		FileUtils.deleteDirectory(new File("tmp/empdir"));
		FileUtils.deleteDirectory(new File("tmp/pridir"));
	}
	
	@Test
	public void testPutPojo(){
		FileStoredMap<Employee> map = new FileStoredMap<Employee>("tmp/empdir");
		Employee emp = createSampleEmp1();
		map.put("emp", emp);
		Employee emp2 = map.get("emp");
		assertThat(emp.getA(), is(emp2.getA()));
		assertThat(emp.getName(), is(emp2.getName()));
		assertThat(emp.getSal(), is(emp2.getSal()));		
	}

	@Test
	public void testPutString(){
		FileStoredMap<String> map = new FileStoredMap<String>("tmp/pridir");
		map.put("foo", "bar");
		String ret = map.get("foo");
		assertThat(ret, is("bar"));
	}
	
	@Test
	public void testPutInt(){
		FileStoredMap<Integer> map = new FileStoredMap<Integer>("tmp/pridir");
		map.put("foo", 1);
		int ret = map.get("foo");
		assertThat(ret, is(1));
	}

	@Test
	public void testPrimitive(){
		assertThat("hoge".getClass().isPrimitive(), is(true));
	}

	protected Employee createSampleEmp1() {
		Employee emp = new Employee();
		emp.setA(1);
		emp.setName("hoge");
		emp.setSal(256);
		return emp;
	}
}
