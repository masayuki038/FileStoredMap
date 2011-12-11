package net.wrap_trap.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import de.undercouch.bson4jackson.BsonFactory;

public class FileStoredMap<K, V> implements Map<K, V> {

	private static final String INDEX_FILE_SUFFIX = ".idx";
	private static final String DATA_FILE_SUFFIX = ".dat";
	
	private static final int INDEX_SIZE_PER_FILE = 429496729; // Integer.MAX_VALUE((2^31-1)/5) 4byte:index file position, 1byte: file index.
	private static final int INDEX_SIZE_PER_RECORD = 5;
	
	private static final int NEXT_DATA_POINTER_SIZE = 5;
	
	private String dirPath;
	
	private Class<?> valueClazz;
	
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
	
	public Set<java.util.Map.Entry<K, V>> entrySet() {
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
			return deserializeValue(buf);
		}catch(IOException ex){
			throw new RuntimeException(ex);
		}
	}
	
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}
	public Set<K> keySet() {
		// TODO Auto-generated method stub
		return null;
	}
	public V put(K key, V value) {
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
				byte[] bytes = serializeValue(value);
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
		}
		return null;
	}
	
	protected byte[] serializeValue(V v) throws JsonGenerationException, JsonMappingException, IOException{
		//Element<V> e = new Element<V>(v);
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectMapper mapper = new ObjectMapper(new BsonFactory());
	    mapper.writeValue(baos, v);
	    return baos.toByteArray();
	}
	
	protected V deserializeValue(byte[] buf) throws JsonGenerationException, JsonMappingException, IOException{
	    ByteArrayInputStream bais = new ByteArrayInputStream(buf);
	    ObjectMapper mapper = new ObjectMapper(new BsonFactory());
	    V ret = (V)mapper.readValue(bais, this.valueClazz);
	    return ret;
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

	public void putAll(Map<? extends K, ? extends V> map) {
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
}
