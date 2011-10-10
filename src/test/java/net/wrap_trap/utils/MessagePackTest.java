package net.wrap_trap.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.MessagePackObject;
import org.msgpack.MessageTypeException;
import org.msgpack.Unpacker;
import org.msgpack.object.ArrayType;
import org.msgpack.object.MapType;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class MessagePackTest {
	
	@Test
	public void testClassSerializing(){
		Employee emp = new Employee();
		emp.setA(76);
		emp.setName("babb");
		emp.setSal(65536);
		
	    byte[] buffer = MessagePack.pack(emp);

	    for (byte b : buffer) {
	      System.out.print(Integer.toHexString(b & 0xFF) + " ");
	    }
	    System.out.println();
	    
	    Employee dst = MessagePack.unpack(buffer, Employee.class);
	    assertThat(dst.getA(), is(76));
	    assertThat(dst.getName(), is("babb"));
	    assertThat(dst.getSal(), is(65536));
	}
	
	@Test
	public void testDeserializingEachValue() throws MessageTypeException, IOException{
		Employee emp = new Employee();
		emp.setA(76);
		emp.setName("babb");
		emp.setSal(65536);
		
	    byte[] buffer = MessagePack.pack(emp);

	    for (byte b : buffer) {
	      System.out.print(Integer.toHexString(b & 0xFF) + " ");
	    }
	    System.out.println();
	    
	    InputStream in = new ByteArrayInputStream(buffer);
	    Unpacker unpacker = new Unpacker(in);
	    
	    for(Object obj : unpacker){
	    	ArrayType array = (ArrayType)obj;
	    	for(Object obj2 : array.asArray()){
	    		System.out.println(obj2);
	    	}
	    }
	}
	
	@Test
	public void testSerliazingMapByMessagePack(){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("int", 1);
		map.put("long", 1L);
		map.put("date", new Date());
		map.put("string", "test");

		byte[] buffer = MessagePack.pack(map);
	    for (byte b : buffer) {
		      System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
		System.out.println();
	    System.out.println("buffer.length: " + buffer.length);
		System.out.println();
	    
		InputStream in = new ByteArrayInputStream(buffer);
	    Unpacker unpacker = new Unpacker(in);
	    
	    for(Object obj : unpacker){
	    	MapType mapType = (MapType)obj;
	    	Map<MessagePackObject, MessagePackObject> ret = mapType.asMap();
	    	for(MessagePackObject key : ret.keySet()){
	    		String keyStr = key.asString();
	    		Object value = ret.get(key);
	    		System.out.println(String.format("%s: %s", keyStr, value));
	    	}
	    }
	}
	    
	@Test
	public void testSerliazingMapByJavaSerialize() throws IOException, ClassNotFoundException{
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("int", 1);
		map.put("long", 1L);
		map.put("date", new Date());
		map.put("string", "test");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(baos);
		os.writeObject(map);
		os.flush();

		byte[] buffer = baos.toByteArray();
	    for (byte b : buffer) {
		      System.out.print(Integer.toHexString(b & 0xFF) + " ");
	    }
		System.out.println();
	    System.out.println("buffer.length: " + buffer.length);
		System.out.println();
	    
		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buffer));
		Map<String, Object> ret = (Map<String, Object>)in.readObject();
		for(String key : ret.keySet()){
			System.out.println(String.format("%s: %s", key, ret.get(key)));
		}
	}
	
	@Test
	public void checkPerformance(){
		Map<Integer, List<Map<String, Object>>> map1 = new HashMap<Integer,  List<Map<String, Object>>>();
		Map<Integer, byte[]> map2 = new HashMap<Integer, byte[]>();

		long start = System.currentTimeMillis();
		for(int i = 0; i < 10000; i++){
			map1.put(i, createMapListNormal());
		}
		System.out.println("createMapListNormal: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		for(int i = 0; i < 10000; i++){
			map2.put(i, createMapListPacked());
		}
		System.out.println("createMapListPacked: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		for(int i = 0; i < 10000; i++){
			map1.get(i);
		}
		System.out.println("createMapListNormal - get: " + (System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		for(int i = 0; i < 10000; i++){
			Unpacker unpacker = new Unpacker(new ByteArrayInputStream(map2.get(i)));
		    for(Object obj : unpacker){
		    	ArrayType arrayType = (ArrayType)obj;
		    }			
		}
		System.out.println("createMapListPacked - get: " + (System.currentTimeMillis() - start));

	}
	
	protected List<Map<String, Object>> createMapListNormal(){
		List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("int", 1);
		map.put("long", 1L);
		map.put("date", new Date());
		map.put("string", "test");
		ret.add(map);

		map = new HashMap<String, Object>();
		map.put("int", Integer.MAX_VALUE);
		map.put("long", Long.MAX_VALUE);
		map.put("date", new Date());
		map.put("string", "test2");
		ret.add(map);

		map = new HashMap<String, Object>();
		map.put("int", Integer.MIN_VALUE);
		map.put("long", Long.MIN_VALUE);
		map.put("date", new Date());
		map.put("string", "test3");
		ret.add(map);
		
		return ret;
	}
	
	protected byte[] createMapListPacked(){
		List<Map<String, Object>> list = createMapListNormal();
		return MessagePack.pack(list);
	}
	
	
}
