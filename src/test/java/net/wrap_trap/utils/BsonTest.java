package net.wrap_trap.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
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
	public void testSerializeStringWithStreamAPI() throws JsonGenerationException, JsonMappingException, IOException{
		BsonFactory f = new BsonFactory();
	    ByteArrayOutputStream baos = null;
	    ByteArrayInputStream bais = null;
	    try{
	    	baos = new ByteArrayOutputStream();
			JsonGenerator g = f.createJsonGenerator(baos);
			g.writeStartObject();
			g.writeObjectField("foo", "bar");
			g.writeEndObject();
			g.close();

			byte[] buffer = baos.toByteArray();
		    for (byte b : buffer) {
			      System.out.print(Integer.toHexString(b & 0xFF) + " ");
			}
		    System.out.println();

	    	String key = null;
	    	String value = null;

	    	bais = new ByteArrayInputStream(buffer);
		    JsonParser p = f.createJsonParser(bais);
		    p.nextToken();
		    while (p.nextToken() != JsonToken.END_OBJECT) {
		    	key = p.getCurrentName();
		    	p.nextToken();
		    	//value = p.readValueAs(new TypeReference<String>(){});
		    	value = (String)p.getEmbeddedObject();
		    }
		    p.close();
		    
		    assertThat(key, is("foo"));
		    assertThat(value, is("bar"));
	    }catch(Exception ex){
	    	throw new RuntimeException(ex);
	    }finally{
	    	if(baos != null){
	    		try{
	    			baos.close();
	    		}catch(IOException ignore){}
	    	}
	    	if(bais != null){
	    		try{
	    			bais.close();
	    		}catch(IOException ignore){}
	    	}
	    }
	
	}
}
