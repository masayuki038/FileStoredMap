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
		try{
			FileUtils.deleteDirectory(new File("tmp/empdir"));
		}catch(IOException ignore){}

		try{
			FileUtils.deleteDirectory(new File("tmp/empdir2"));
		}catch(IOException ignore){}
		
		try{
			FileUtils.deleteDirectory(new File("tmp/pridir"));
		}catch(IOException ignore){}
	}
	
	@Test
	public void testPutNestedPojo(){
		FileStoredMap<Employer> map = new FileStoredMap<Employer>("tmp/empdir2");
		Employee emp1 = createEmployee(1, "foo", 256);
		Employee emp2 = createEmployee(2, "bar", 65536);
		Employer employer = createEmployer("boss");
		employer.addEmployee(emp1);
		employer.addEmployee(emp2);
		
		map.put("employer", employer);
		Employer rEmployer = map.get("employer");
		assertThat(rEmployer.getName(), is(employer.getName()));
		assertThat(rEmployer.getEmpList().size(), is(2));
		
		Employee rEmp1 = rEmployer.getEmpList().get(0);
		assertThat(rEmp1.getA(), is(emp1.getA()));
		assertThat(rEmp1.getName(), is(emp1.getName()));
		assertThat(rEmp1.getSal(), is(emp1.getSal()));		

		Employee rEmp2 = rEmployer.getEmpList().get(1);
		assertThat(rEmp2.getA(), is(emp2.getA()));
		assertThat(rEmp2.getName(), is(emp2.getName()));
		assertThat(rEmp2.getSal(), is(emp2.getSal()));		
}
	
	@Test
	public void testPutPojo(){
		FileStoredMap<Employee> map = new FileStoredMap<Employee>("tmp/empdir");
		Employee emp = createEmployee(1, "hoge", 256);
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

	protected Employee createEmployee(int a, String name, int sal) {
		Employee employee = new Employee();
		employee.setA(a);
		employee.setName(name);
		employee.setSal(sal);
		return employee;
	}
	
	protected Employer createEmployer(String name){
		Employer employer = new Employer();
		employer.setName(name);
		return employer;
	}
}
