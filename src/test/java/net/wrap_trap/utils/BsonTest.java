package net.wrap_trap.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import de.undercouch.bson4jackson.BsonFactory;

public class BsonTest {

	@Test
	public void testSerialize() throws JsonGenerationException, JsonMappingException, IOException{
		Employee emp = new Employee();
		emp.setA(76);
		emp.setName("babb");
		emp.setSal(Integer.MAX_VALUE-2);

	    //serialize data
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectMapper mapper = new ObjectMapper(new BsonFactory());
	    mapper.writeValue(baos, emp);
	    byte[] buffer = baos.toByteArray();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
		Employee dst = mapper.readValue(bais, Employee.class);
		assertThat(dst.getA(), is(76));
		assertThat(dst.getName(), is("babb"));
		assertThat(dst.getSal(), is(Integer.MAX_VALUE-2));
	}
	
	@Test
	public void testSerializeString() throws JsonGenerationException, JsonMappingException, IOException{
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectMapper mapper = new ObjectMapper(new BsonFactory());
	    mapper.writeValue(baos, "bar");
	    byte[] buffer = baos.toByteArray();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
		Object obj = mapper.readValue(bais, String.class);
		assertThat((obj instanceof String), is(true));
		assertThat((String)obj, is("bar"));
	}
}
