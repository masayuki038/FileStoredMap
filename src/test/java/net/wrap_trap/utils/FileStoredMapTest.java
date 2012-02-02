package net.wrap_trap.utils;

import java.io.IOException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class FileStoredMapTest {
	
	@Before
	public void setUp() throws IOException{
	}

	@Test
	public void testPutNestedPojo(){
		TestUtils.deleteFiles("tmp/empdir2");
		long start = System.currentTimeMillis();
		FileStoredMap<Employer> map = null;
		try{
			map = new FileStoredMap<Employer>("tmp/empdir2");
			Employee emp1 = TestUtils.createEmployee("foo", 256, new Date());
			Employee emp2 = TestUtils.createEmployee("bar", 65536, new Date());
			Employer employer = TestUtils.createEmployer("boss");
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

			TestUtils.assertEmployeeEquivalent(emp1, rEmployer.getEmpList().get(0));
			TestUtils.assertEmployeeEquivalent(emp2, rEmployer.getEmpList().get(1));
		}finally{
			if(map != null){
				map.close();
			}
		}
	}
	
	@Test
	public void testPutPojo(){
		TestUtils.deleteFiles("tmp/empdir");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/empdir");
			Date createdAt = new Date();
			Employee emp = TestUtils.createEmployee("hoge", 256, createdAt);
			map.put("emp", emp);
			TestUtils.assertEmployeeEquivalent(emp, map.get("emp"));
		}finally{
			if(map != null){
				map.close();
			}
		}
	}

	@Test
	public void testPutString(){
		TestUtils.deleteFiles("tmp/pridir");
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
		TestUtils.deleteFiles("tmp/pridir");
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
		TestUtils.deleteFiles("tmp/remove1");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove1");
			Employee emp = TestUtils.createEmployee("hoge", 1, new Date());
			map.put("emp", emp);
			assertThat(map.get("emp2"), nullValue());
		}finally{
			if(map != null){
				map.close();
			}
		}
	}

	@Test
	public void testRemoveNoIndexFile(){
		TestUtils.deleteFiles("tmp/remove2");
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
		TestUtils.deleteFiles("tmp/remove3");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove3");
			Employee emp = TestUtils.createEmployee("hoge", 256, new Date());
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
		TestUtils.deleteFiles("tmp/remove4");
		FileStoredMap<Employee> map = null;
		try{
			map = new FileStoredMap<Employee>("tmp/remove4");
			Date createdAt = new Date();
			Employee emp = TestUtils.createEmployee("hoge", 256, createdAt);
			map.put("emp", emp);
			TestUtils.assertEmployeeEquivalent(emp, map.remove("emp"));
			assertThat(map.get("emp"), nullValue());
		}finally{
			if(map != null){
				map.close();
			}
		}
	}
}
