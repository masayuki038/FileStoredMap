package net.wrap_trap.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

import net.wrap_trap.monganez.BSONObjectMapper;

public class FileStoredMap<V> implements Map<String, V> {

	private static final String INDEX_FILE_SUFFIX = ".idx";
	private static final String DATA_FILE_SUFFIX = ".dat";
	
	private static final int INDEX_SIZE_PER_FILE = 429496729; // Integer.MAX_VALUE((2^31-1)/5) 4byte:index file position, 1byte: file index.
	private static final int INDEX_SIZE_PER_RECORD = 5;
	
	private static final int NEXT_DATA_POINTER_SIZE = 5;
	
	private String dirPath;
	
	private BasicBSONEncoder encoder = new BasicBSONEncoder();
	private BasicBSONDecoder decoder = new BasicBSONDecoder();
	private BSONObjectMapper objectMapper = new BSONObjectMapper();

	
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
		throw new NotImplementedException();
	}
	public V get(Object key) {
		int hashCode = key.hashCode();
		int idx = hashCode / INDEX_SIZE_PER_FILE + 1;
		int pos = hashCode % INDEX_SIZE_PER_FILE;
		
		RandomAccessFile indexFile = null;
		try{
			String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
			indexFile = new RandomAccessFile(indexFilePath, "r");
			byte[] buf = readFrom(indexFile, pos);
			return rebuildValue(buf);
		}catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (FileNotFoundException ex) {
			throw new RuntimeException(ex);
		} finally {
			Closeables.closeQuietly(indexFile);
		}
	}

	protected byte[] readFrom(RandomAccessFile indexFile, int pos){
		RandomAccessFile dataFile = null;
		try{
			indexFile.seek(pos);
			int dataPos = indexFile.readInt();
			String dataFilePath = getDataFilePath(indexFile.readByte());
			dataFile = new RandomAccessFile(dataFilePath, "r");
			dataFile.seek(dataPos);
			int dataLength = dataFile.readInt();
			byte[] buf = new byte[dataLength];
			if(dataFile.read(buf) < dataLength){
				throw new RuntimeException("error");
			}
			return buf;
		}catch(IOException ex){
			throw new RuntimeException(ex);
		}finally{
			Closeables.closeQuietly(dataFile);
		}
	}
	public boolean isEmpty() {
		throw new NotImplementedException();
	}
	public Set<String> keySet() {
		throw new NotImplementedException();
	}
	public V put(String key, V value) {
		int hashCode = key.hashCode();
		int idx = hashCode / INDEX_SIZE_PER_FILE + 1;
		int pos = hashCode % INDEX_SIZE_PER_FILE;
		
		RandomAccessFile indexFile = null;
		try{
			String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
			indexFile = new RandomAccessFile(indexFilePath, "rw");
			indexFile.seek(pos);
			byte[] buf = new byte[INDEX_SIZE_PER_RECORD];
			int read = indexFile.read(buf, 0, INDEX_SIZE_PER_RECORD);
			if(read == INDEX_SIZE_PER_RECORD){
				throw new NotImplementedException("already exists.");
			}else if(read < INDEX_SIZE_PER_RECORD){
				// no exists.
				writeTo(indexFile, key, value);
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}finally{
			Closeables.closeQuietly(indexFile);
		}
		return null;
	}

	protected void writeTo(RandomAccessFile indexFile, String key, V value) {
		try {
			byte[] bytes = toByteArray(key, value);
			writeTo(indexFile, bytes);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	protected void writeTo(RandomAccessFile indexFile, byte[] bytes) {
		RandomAccessFile dataFile = null;
		try{
			byte lastDataFileNumber = getLastDataFileNumber();
			String dataFilePath = getDataFilePath(lastDataFileNumber);
			dataFile = new RandomAccessFile(dataFilePath, "rw");

			long dataPos = dataFile.length();
			dataFile.seek(dataPos);
			dataFile.writeInt(bytes.length + NEXT_DATA_POINTER_SIZE);
			dataFile.write(bytes);
			dataFile.writeInt(0); // the file position of next data.
			dataFile.writeByte(0); // the file number of next data. 
			indexFile.writeInt((int)dataPos);
			indexFile.writeByte(lastDataFileNumber);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}finally{
			Closeables.closeQuietly(dataFile);
		}
	}
	
	protected byte[] toByteArray(String key, V v) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		BSONObject object = objectMapper.createBSONObject(key, v);
		return encoder.encode(object);
	}
	
	@SuppressWarnings("unchecked")
	protected V rebuildValue(byte[] buf) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException {
		BSONObject object = decoder.readObject(buf);

		Set<String> keySet = object.keySet();
		Preconditions.checkArgument(keySet.size() == 1);

		for(String key : keySet){ 
			Object target = object.get(key);
			Object v = null;
			if(target instanceof BSONObject){
				v = objectMapper.toObject((BSONObject)target);
			}else{
				v = target;
			}
			return (V)v;
		}
		return null;
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
		throw new NotImplementedException();
	}
	public V remove(Object arg0) {
		throw new NotImplementedException();
	}
	public int size() {
		throw new NotImplementedException();
	}
	public Collection<V> values() {
		throw new NotImplementedException();
	}
	
	protected void dump(byte[] bin){
	    for (byte b : bin) {
		      System.out.print(Integer.toHexString(b & 0xFF) + " ");
		}
	    System.out.println();
	}
}
