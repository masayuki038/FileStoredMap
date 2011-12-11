package net.wrap_trap.utils;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class FileStoredMapTest {	
	@Test
	public void testPutPojo(){
		FileStoredMap<String, Employee> map = new FileStoredMap<String, Employee>("tmp/empdir");
		Employee emp = createSampleEmp1();
		map.put("emp", emp);
		Employee emp2 = map.get("emp");
		assertThat(emp.getA(), is(emp2.getA()));
		assertThat(emp.getName(), is(emp2.getName()));
		assertThat(emp.getSal(), is(emp2.getSal()));		
	}

	protected Employee createSampleEmp1() {
		Employee emp = new Employee();
		emp.setA(1);
		emp.setName("hoge");
		emp.setSal(256);
		return emp;
	}
}
