package net.wrap_trap.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.PropertyUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.node.BinaryNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DecimalNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.codehaus.jackson.node.ValueNode;
import org.codehaus.jackson.type.JavaType;

import de.undercouch.bson4jackson.BsonFactory;

public class FileStoredMap<V> implements Map<String, V> {

	private static final String INDEX_FILE_SUFFIX = ".idx";
	private static final String DATA_FILE_SUFFIX = ".dat";
	
	private static final int INDEX_SIZE_PER_FILE = 429496729; // Integer.MAX_VALUE((2^31-1)/5) 4byte:index file position, 1byte: file index.
	private static final int INDEX_SIZE_PER_RECORD = 5;
	
	private static final int NEXT_DATA_POINTER_SIZE = 5;
	
	private String dirPath;
	
	private Class<?> valueClazz;
	
	private BsonFactory f = new BsonFactory();
	private ObjectMapper objectMapper = new ObjectMapper(f);
	
	
	public FileStoredMap(String dirPath){
		this.dirPath = dirPath;
		File dir = new File(this.dirPath);
		dir.mkdir();
	}
	
	public void clear() {
		throw new NotImplementedException();
	}
	
	public boolean containsKey(Object key) {
		throw new NotImplementedException();
	}
	
	public boolean containsValue(Object value) {
		throw new NotImplementedException();
	}
	
	public Set<java.util.Map.Entry<String, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}
	public V get(Object key) {
		int hashCode = key.hashCode();
		int idx = hashCode / INDEX_SIZE_PER_FILE + 1;
		int pos = hashCode % INDEX_SIZE_PER_FILE;
		
		try{
			String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
			RandomAccessFile indexFile = new RandomAccessFile(indexFilePath, "r");
			indexFile.seek(pos);
			int dataPos = indexFile.readInt();
			String dataFilePath = getDataFilePath(indexFile.readByte());
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "r");
			dataFile.seek(dataPos);
			int dataLength = dataFile.readInt();
			byte[] buf = new byte[dataLength];
			if(dataFile.read(buf) < dataLength){
				throw new RuntimeException("error");
			}
			return deserializeValue(buf).getValue();
		}catch(IOException ex){
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}
	public Set<String> keySet() {
		// TODO Auto-generated method stub
		return null;
	}
	public V put(String key, V value) {
		this.valueClazz = value.getClass();
		int hashCode = key.hashCode();
		int idx = hashCode / INDEX_SIZE_PER_FILE + 1;
		int pos = hashCode % INDEX_SIZE_PER_FILE;
		
		try{
			String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
			RandomAccessFile indexFile = new RandomAccessFile(indexFilePath, "rw");
			indexFile.seek(pos);
			byte[] buf = new byte[INDEX_SIZE_PER_RECORD];
			int read = indexFile.read(buf, 0, INDEX_SIZE_PER_RECORD);
			if(read == INDEX_SIZE_PER_RECORD){
				// already exists.
				
			}else if(read < INDEX_SIZE_PER_RECORD){
				// no exists.
				byte[] bytes = serializeValue(key, value);
				byte lastDataFileNumber = getLastDataFileNumber();
				String dataFilePath = getDataFilePath(lastDataFileNumber);
				RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
				long dataPos = dataFile.length();
				dataFile.seek(dataPos);
				//dataFile.setLength(dataFile.length() + bytes.length + NEXT_DATA_POINTER_SIZE);
				dataFile.writeInt(bytes.length + NEXT_DATA_POINTER_SIZE);
				dataFile.write(bytes);
				dataFile.writeInt(0); // the file position of next data.
				dataFile.writeByte(0); // the file number of next data. 
				indexFile.writeInt((int)dataPos);
				indexFile.writeByte(lastDataFileNumber);
				dataFile.close();
				indexFile.close();
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}
		return null;
	}
	
	protected byte[] serializeValue(String key, V v) throws JsonGenerationException, JsonMappingException, IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{
		Element<V> e = new Element<V>(key, v);
		return serializeValue(e);
	}
	
	protected byte[] serializeValue(Element<V> e) throws JsonGenerationException, JsonMappingException, IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{
		ByteArrayOutputStream baos = null;
	    try{
		    baos = new ByteArrayOutputStream();
		    ObjectNode root = objectMapper.createObjectNode();		    
		    ObjectNode container = objectMapper.createObjectNode();
		    JavaType javaType = TypeFactory.type(e.getValue().getClass());
			if(javaType.isPrimitive() || (e.getValue() instanceof java.lang.String)){
				container.put("value", createNodeForPrimitive(e.getValue()));
			}else{
			    ObjectNode value = encodeObjectNode(e.getValue());
			    container.put("value", value);				
			}
		    container.put("type", e.getType());
		    root.put(e.getKey(), container);
		    objectMapper.writeValue(baos, root);
		    return baos.toByteArray();
	    }finally{
	    	if(baos != null){
	    		try{
	    			baos.close();
	    		}catch(IOException ignore){}
	    	}
	    }
	}

	@SuppressWarnings("unchecked")
	protected ObjectNode encodeObjectNode(Object value) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		ObjectNode node = objectMapper.createObjectNode();
		Map nestedMap = PropertyUtils.describe(value);
		for(Object o : nestedMap.entrySet()){
			Map.Entry e = (Map.Entry)o;
//			node.putPOJO((String)e.getKey(), e.getValue());
			String k = (String)e.getKey();
			Object v = e.getValue();
			putSimpleValueToNode(node, k, v);
		}
		return node;
	}

	protected void putSimpleValueToNode(ObjectNode node, String k, Object v)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		if(k.equals("class")){
			return; // ignore value of getClass().
		}
		if(v instanceof BigDecimal){
			node.put(k, (BigDecimal)v);
		}else if(v instanceof Boolean){
			node.put(k, (Boolean)v);
		}else if(v instanceof byte[]){
			node.put(k, (byte[])v);
		}else if(v instanceof Double){
			node.put(k, (Double)v);
		}else if(v instanceof Float){
			node.put(k, (Float)v);
		}else if(v instanceof Integer){
			node.put(k, (Integer)v);
		}else if(v instanceof Long){
			node.put(k, (Long)v);
		}else if(v instanceof String){
			node.put(k, (String)v);
		}else if(v instanceof Serializable){
			node.put(k, encodeObjectNode(v));
		}else{
			throw new RuntimeException("unexpected type: " + v.getClass());
		}
	}
	
	protected ValueNode createNodeForPrimitive(Object v){
		if(v instanceof BigDecimal){
			return new DecimalNode((BigDecimal)v);
		}else if(v instanceof Boolean){
			return BooleanNode.valueOf((Boolean)v);
		}else if(v instanceof byte[]){
			return new BinaryNode((byte[])v);
		}else if(v instanceof Double){
			return DoubleNode.valueOf((Double)v);
		}else if(v instanceof Float){
			return DoubleNode.valueOf((Float)v);
		}else if(v instanceof Integer){
			return IntNode.valueOf((Integer)v);
		}else if(v instanceof Long){
			return LongNode.valueOf((Long)v);
		}else if(v instanceof String){
			return new TextNode((String)v);
		}else{
			throw new RuntimeException("unexpected type: " + v.getClass());
		}
	}

	protected Element<V> deserializeValue(byte[] buf) throws JsonGenerationException, JsonMappingException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException{
	    ByteArrayInputStream bais = null;
	    try{
	    	bais = new ByteArrayInputStream(buf);
	    	JsonNode root = objectMapper.readValue(bais, JsonNode.class);
	    	
	    	String k = null;
	    	for(Iterator<String> i = root.getFieldNames(); i.hasNext();){
	    		k = i.next();
	    	}
	    	
	    	JsonNode container = root.get(k);
	    	JsonNode value = container.get("value");
	    	JsonNode type = container.get("type");
	    	Object obj = deserializeValue(value, type.getTextValue());
	    	return new Element<V>(k, (V)obj);
	    }finally{
	    	if(bais != null){
	    		try{
	    			bais.close();
	    		}catch(IOException ignore){}
	    	}
	    }
	}

	protected Object deserializeValue(JsonNode node, String type) throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IOException {
		boolean primitiveType = node.isValueNode();
		if(primitiveType){
			if(node.isBigDecimal()){
				return node.getDecimalValue();
			}else if (node.isBigInteger()){
				return node.getBigIntegerValue();
			}else if(node.isBinary()){
				return node.getBinaryValue();
			}else if(node.isBoolean()){
				return node.getBooleanValue();
			}else if(node.isDouble()){
				return node.getDoubleValue();
			}else if(node.isFloatingPointNumber()){
				return node.getDoubleValue();
			}else if(node.isInt()){
				return node.getIntValue();
			}else if(node.isLong()){
				return node.getLongValue();				
			}else if(node.isTextual()){
				return node.getTextValue();
			}else{
				throw new RuntimeException("unexpected type. field name: " + node.getFieldNames().next());
			}
		}else{
			Object obj = Class.forName(type).newInstance();
			for(Iterator<String> i = node.getFieldNames(); i.hasNext();){
				String k = i.next();
				JsonNode v = node.get(k);
				if(v.isBigDecimal()){
					PropertyUtils.setProperty(obj, k, v.getDecimalValue());
				}else if (v.isBigInteger()){
					PropertyUtils.setProperty(obj, k, v.getBigIntegerValue());
				}else if(v.isBinary()){
					PropertyUtils.setProperty(obj, k, v.getBinaryValue());
				}else if(v.isBoolean()){
					PropertyUtils.setProperty(obj, k, v.getBooleanValue());
				}else if(v.isDouble()){
					PropertyUtils.setProperty(obj, k, v.getDoubleValue());
				}else if(v.isFloatingPointNumber()){
					PropertyUtils.setProperty(obj, k, v.getDoubleValue());
				}else if(v.isInt()){
					PropertyUtils.setProperty(obj, k, v.getIntValue());
				}else if(v.isLong()){
					PropertyUtils.setProperty(obj, k, v.getLongValue());
				}else if(v.isTextual()){
					PropertyUtils.setProperty(obj, k, v.getTextValue());
				}else{
					throw new RuntimeException("unexpected type. property name: " + k);
				}
			}
			return obj;			
		}
	}

	protected byte getLastDataFileNumber(){
		byte i = 2;
		while(true){
			String dataFilePath = dirPath + File.separator + Integer.toString(i) + DATA_FILE_SUFFIX;
			if(!new File(dataFilePath).exists()){
				return --i;
			}
		}
	}
	
	protected String getDataFilePath(byte fileNumber) {
		return dirPath + File.separator + Integer.toString(fileNumber) + DATA_FILE_SUFFIX;
	}

	public void putAll(Map<? extends String, ? extends V> map) {
		// TODO Auto-generated method stub
		
	}
	public V remove(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}
	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}
	
	protected void dump(byte[] bin){
	    for (byte b : bin) {
		      System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
	    System.out.println();
	}
}
