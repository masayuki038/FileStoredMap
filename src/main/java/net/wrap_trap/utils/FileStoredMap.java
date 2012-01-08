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

	/**
	 * 	structure of index
	 * +--+--+--+--+--+
	 * |     a.    |b.|
	 * +--+--+--+--+--+
	 * 
	 * a. a offset of data file.[integer]
	 * b. data file number(1-2).[byte]
	 */
	private static final String INDEX_FILE_SUFFIX = ".idx";

	/**
	 * 	structure of data
	 * +--+--+--+--+--+--+--+--+--+--+--+--+--+
	 * |     a.    |     b.    |     c.    |d.|  
	 * +--+--+--+--+--+--+--+--+--+--+--+--+--+
	 * 
	 * a. a data length(from b. to d.)[integer]
	 * b. data[byte[]]
	 * c. a file position of next data.[integer]
	 * d. a file number of next data.[byte]
	 */
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
			indexFile = openIndexFile(idx, "r");
			if(indexFile == null){
				return null;
			}
			// TODO Need to specify the key to check the equivalent of the key for hash code collision.
			byte[] buf = readFrom(indexFile, pos);
			if(buf == null || buf.length == 0){
				return null;
			}
			return rebuildValue(buf);
		} catch (FileNotFoundException ex) {
			return null;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} finally {
			Closeables.closeQuietly(indexFile);
		}
	}

	protected byte[] readFrom(RandomAccessFile indexFile, int pos) throws IOException, FileNotFoundException{
		if(indexFile.length() < pos + INDEX_SIZE_PER_RECORD){
			return null;
		}
		indexFile.seek(pos);
		int dataPos = indexFile.readInt();
		byte fileNumber = indexFile.readByte();
		if(dataPos == 0 && fileNumber == 0){
			// index record is empty.(this record area has cleaned up.)
			return null;
		}
		return readFrom(dataPos, fileNumber);
	}

	protected byte[] readFrom(int dataPos, byte fileNumber) throws IOException, FileNotFoundException {
		RandomAccessFile dataFile = null;
		try {
			String dataFilePath = getDataFilePath(fileNumber);
			dataFile = new RandomAccessFile(dataFilePath, "r");
			return readDataFile(dataFile, dataPos);
		} finally {
			Closeables.closeQuietly(dataFile);
		}
	}

	protected byte[] readDataFile(RandomAccessFile dataFile, int dataPos)
			throws IOException {
		dataFile.seek(dataPos);
		int dataLength = dataFile.readInt();
		int bodySize = dataLength - NEXT_DATA_POINTER_SIZE;
		byte[] buf = new byte[bodySize];
		if(dataFile.read(buf) < bodySize){
			throw new RuntimeException("error");
		}
		return buf;
	}
	public boolean isEmpty() {
		throw new NotImplementedException();
	}
	public Set<String> keySet() {
		throw new NotImplementedException();
	}
	
	public V remove(Object key) {
		int hashCode = key.hashCode();
		int idx = hashCode / INDEX_SIZE_PER_FILE + 1;
		int pos = hashCode % INDEX_SIZE_PER_FILE;
		
		RandomAccessFile indexFile = null;
		RandomAccessFile dataFile = null;
		try{
			indexFile = openIndexFile(idx, "rw");
			if(indexFile.length() < pos + INDEX_SIZE_PER_RECORD){
				return null;
			}
			indexFile.seek(pos);
			int dataPos = indexFile.readInt();
			String dataFilePath = getDataFilePath(indexFile.readByte());
			dataFile = new RandomAccessFile(dataFilePath, "r");
			dataFile.seek(dataPos);
			int dataLength = dataFile.readInt();
			dataFile.seek(dataPos + 4/* size of dataLength filed */ + dataLength - NEXT_DATA_POINTER_SIZE);
			int nextDataPos = dataFile.readInt();
			if(nextDataPos == 0){
				clearIndex(indexFile, pos);
			}else{
				updateIndex(indexFile, pos, nextDataPos, dataFile.readByte());
			}
			byte[] body = readDataFile(dataFile, dataPos);
			return rebuildValue(body);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}finally{
			Closeables.closeQuietly(dataFile);
			Closeables.closeQuietly(indexFile);
		}
	}
	
	protected void clearIndex(RandomAccessFile indexFile, int pos) throws IOException{
		indexFile.seek(pos);
		indexFile.write(new byte[INDEX_SIZE_PER_RECORD]);
	}
	
	protected void updateIndex(RandomAccessFile indexFile, int indexPos, int nextDataPos, byte nextDataFileNumber) throws IOException{
		indexFile.seek(indexPos);
		indexFile.writeInt(nextDataPos);
		indexFile.writeByte(nextDataFileNumber);
	}
	
	public V put(String key, V value) {
		int hashCode = key.hashCode();
		int idx = hashCode / INDEX_SIZE_PER_FILE + 1;
		int pos = hashCode % INDEX_SIZE_PER_FILE;
		
		RandomAccessFile indexFile = null;
		try {
			indexFile = openIndexFile(idx, "rw");
			if(containsKey(indexFile, pos)){
				throw new NotImplementedException("already exists.");
			} else {
				writeTo(indexFile, key, value);
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}finally{
			Closeables.closeQuietly(indexFile);
		}
		return null;
	}

	private RandomAccessFile openIndexFile(int idx, String flag)
			throws FileNotFoundException {
		String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
		if("r".equals(flag)){
			File file = new File(indexFilePath);
			if(!file.exists()){
				return null;
			}
		}
		return new RandomAccessFile(indexFilePath, flag);
	}
	
	protected boolean containsKey(RandomAccessFile indexFile, int pos) throws IOException{
		indexFile.seek(pos);
		byte[] buf = new byte[INDEX_SIZE_PER_RECORD];
		int read = indexFile.read(buf, 0, INDEX_SIZE_PER_RECORD);
		// TODO re-think to check the way that the specified key is registered in index file.
		return !(read < INDEX_SIZE_PER_RECORD);
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
			dataFile.writeByte(0); // the file position of next data.
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
	protected V rebuildValue(byte[] buf) {
		BSONObject object = decoder.readObject(buf);

		try{
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
		}catch(IllegalAccessException ex){
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex);
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
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
