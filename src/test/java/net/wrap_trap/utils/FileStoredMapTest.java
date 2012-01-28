package net.wrap_trap.utils;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.legacy.PowerMockRunner;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class FileStoredMapTest {
	
	@Before
	public void setUp() throws IOException{
	}

	private void deleteFiles(String path) {
		try{
			FileUtils.deleteDirectory(new File(path));
		}catch(IOException ignore){}
	}
	
	@Test
	public void testPutNestedPojo(){
		deleteFiles("tmp/empdir2");
		long start = System.currentTimeMillis();
		FileStoredMap<Employer> map = null;
		try{
			map = new FileStoredMap<Employer>("tmp/empdir2");
			Employee emp1 = createEmployee("foo", 256, new Date());
			Employee emp2 = createEmployee("bar", 65536, new Date());
			Employer employer = createEmployer("boss");
			employer.addEmployee(emp1);
			employer.addEmployee(emp2);
			long c1 = System.currentTimeMillis();
			System.out.println(String.format("c1 - start: %d", c1 - start));
			
			map.put("employer", employer);
			long c2 = System.currentTimeMillis();
			System.out.println(String.format("c2 - c1: %d", c2 - c1));

			Employer rEmployer = map.get("employer");
			long c3 = System.currentTimeMillis();
			System.out.println(String.format("c3 - c2: %d", c3 - c2));

			assertThat(rEmployer.getName(), is(employer.getName()));
			assertThat(rEmployer.getEmpList().size(), is(2));
			
			Employee rEmp1 = rEmployer.getEmpList().get(0);
			assertThat(rEmp1.getName(), is(emp1.getName()));
			assertThat(rEmp1.getSal(), is(emp1.getSal()));		
			assertThat(rEmp1.getCreatedAt(), is(emp1.getCreatedAt()));

			Employee rEmp2 = rEmployer.getEmpList().get(1);
			assertThat(rEmp2.getCreatedAt(), is(emp2.getCreatedAt()));
			assertThat(rEmp2.getName(), is(emp2.getName()));
			assertThat(rEmp2.getSal(), is(emp2.getSal()));		
		}finally{
			if(map != null){
				map.close();
			}
		}
	}
	
	@Test
	public void testPutPojo(){
		deleteFiles("tmp/empdir");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/empdir");
			Date createdAt = new Date();
			Employee emp = createEmployee("hoge", 256, createdAt);
			map.put("emp", emp);
			Employee emp2 = map.get("emp");
			assertThat(emp.getCreatedAt(), is(emp2.getCreatedAt()));
			assertThat(emp.getName(), is(emp2.getName()));
			assertThat(emp.getSal(), is(emp2.getSal()));		
		}finally{
			if(map != null){
				map.close();
			}
		}
	}

	@Test
	public void testPutString(){
		deleteFiles("tmp/pridir");
		FileStoredMap<String> map = null;
		try{
			map = new FileStoredMap<String>("tmp/pridir");
			map.put("foo", "bar");
			String ret = map.get("foo");
			assertThat(ret, is("bar"));
		}finally{
			if(map != null){
				map.close();
			}
		}
	}
	
	@Test
	public void testPutInt(){
		deleteFiles("tmp/pridir");
		FileStoredMap<Integer> map = null;
		try{
			map = new FileStoredMap<Integer>("tmp/pridir");
			map.put("foo", 1);
			int ret = map.get("foo");
			assertThat(ret, is(1));
		}finally{
			if(map != null){
				map.close();
			}
		}
	}
	
	@Test
	public void testGetNull(){
		deleteFiles("tmp/remove1");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove1");
		}finally{
			if(map != null){
				Employee emp = createEmployee("hoge", 1, new Date());
				map.put("emp", emp);
				assertThat(map.get("emp2"), nullValue());
			}
		}
	}

	@Test
	public void testRemoveNoIndexFile(){
		deleteFiles("tmp/remove2");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove2");
			assertThat(map.get("emp2"), nullValue());
		}finally{
			if(map != null){
				map.close();
			}
		}
	}

	@Test
	public void testRemoveNull(){
		deleteFiles("tmp/remove3");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove3");
			Employee emp = createEmployee("hoge", 256, new Date());
			map.put("emp", emp);
			assertThat(map.remove("emp2"), nullValue());
		}finally{
			if(map != null){
				map.close();
			}
		}
	}

	@Test
	public void testRemove(){
		deleteFiles("tmp/remove4");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove4");
			Date createdAt = new Date();
			Employee emp = createEmployee("hoge", 256, createdAt);
			map.put("emp", emp);
			Employee emp2 = map.remove("emp");
			assertThat(emp.getCreatedAt(), is(emp2.getCreatedAt()));
			assertThat(emp.getName(), is(emp2.getName()));
			assertThat(emp.getSal(), is(emp2.getSal()));
			assertThat(map.get("emp"), nullValue());
		}finally{
			if(map != null){
				map.close();
			}
		}
	}
	
	protected Employee createEmployee(String name, int sal, Date created) {
		Employee employee = new Employee();
		employee.setName(name);
		employee.setSal(sal);
		employee.setCreatedAt(created);
		return employee;
	}
	
	protected Employer createEmployer(String name){
		Employer employer = new Employer();
		employer.setName(name);
		return employer;
	}
}
