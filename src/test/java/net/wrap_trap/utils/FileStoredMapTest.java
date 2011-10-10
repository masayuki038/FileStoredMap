package net.wrap_trap.utils;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class FileStoredMapTest {
	@Test
	public void testPutGetString() throws JsonGenerationException, JsonMappingException, IOException{
		FileStoredMap<String, String> map = new FileStoredMap<String, String>("testdir");
		map.put("test1", "bar");
		String ret = map.get("test1");
		assertThat(ret, is("bar"));
	}
}
