package net.wrap_trap.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.wrap_trap.monganez.BSONObjectMapper;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

public class FileStoredMap<V> implements Map<String, V> {

    /**
     * <pre>
     * 	structure of index
     * +--+--+--+--+--+
     * |     a.    |b.|
     * +--+--+--+--+--+
     * 
     * a. a offset of data file.[integer]
     * b. data file number(1-2).[byte]
     * </pre>
     */
    private static final String INDEX_FILE_SUFFIX = ".idx";

    /**
     * <pre>
     * 	structure of data
     * +--+--+--+--+--+--+--+--+--+--+--+--+--+
     * |     a.    |     b.    |     c.    |d.|  
     * +--+--+--+--+--+--+--+--+--+--+--+--+--+
     * 
     * a. a data length(from b. to d.)[integer]
     * b. data[byte[]]
     * c. a file position of next data.[integer]
     * d. a file number of next data.[byte]
     * </pre>
     */
    private static final String DATA_FILE_SUFFIX = ".dat";

    private static final int INDEX_SIZE_PER_FILE = 429496729; // Integer.MAX_VALUE(2^31-1)/5 4byte:index file position, 1byte: file index.
    private static final int INDEX_SIZE_PER_RECORD = 5;

    private static final int DATA_LENGTH_FIELD_SIZE = 4; // (a.) size of integer.
    private static final int NEXT_DATA_POINTER_SIZE = 5;

    private static final int MAX_NUMBER_OF_DATA_FILES = 2;
    private static final int MAX_NUMBER_OF_INDEX_FILES = 10; // Integer.MAX_VALUE(2^31)/INDEX_SIZE_PER_FILE*2(negative/positive areas of integer)

    private String dirPath;

    private BasicBSONEncoder encoder = new BasicBSONEncoder();
    private BasicBSONDecoder decoder = new BasicBSONDecoder();
    private BSONObjectMapper objectMapper = new BSONObjectMapper();

    private RandomAccessFile[] dataFiles = new RandomAccessFile[MAX_NUMBER_OF_DATA_FILES];
    private RandomAccessFile[] indexFiles = new RandomAccessFile[MAX_NUMBER_OF_INDEX_FILES];

    protected static Logger logger = LoggerFactory.getLogger(FileStoredMap.class);

    public FileStoredMap(String dirPath) {
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
        if (logger.isTraceEnabled()) {
            logger.trace("get, key:{}", key);
        }
        return getInternal(key);
    }

    protected V getInternal(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("getInternal, key:{}", key);
        }
        long hashCode = toUnsignedInt(key.hashCode());
        int idx = (int) (hashCode / INDEX_SIZE_PER_FILE) + 1;
        int pos = (int) (hashCode % INDEX_SIZE_PER_FILE);

        RandomAccessFile indexFile = null;
        try {
            indexFile = getIndexFile(idx);
            if (indexFile == null)
                return null;
            BSONObject bsonObject = readFrom(key.toString(), indexFile, pos);
            if (bsonObject == null)
                return null;
            return rebuildValue(bsonObject);
        } catch (FileNotFoundException ex) {
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected BSONObject readFrom(String key, RandomAccessFile indexFile, int pos) throws IOException,
            FileNotFoundException {
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return null;
        indexFile.seek(pos);
        int dataPos = indexFile.readInt();
        byte fileNumber = indexFile.readByte();
        return readFrom(key, dataPos, fileNumber);
    }

    protected BSONObject readFrom(String key, int dataPos, byte fileNumber) throws IOException, FileNotFoundException {
        if (logger.isTraceEnabled()) {
            logger.trace("readFrom, key:{}, dataPos:{}, fileNumber:{}", new Object[] { key, dataPos, fileNumber });
        }
        if (dataPos == 0 && fileNumber == 0)
            // index record is empty.(this record area has cleaned up.)
            return null;
        return readDataFile(key, dataPos, fileNumber);
    }

    protected BSONObject readDataFile(String key, int dataPos, byte fileNumber) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("readDataFile, key:{}, dataPos:{}, fileNumber:{}", new Object[] { key, dataPos, fileNumber });
        }
        DataRef dataRef = getDataRef(dataPos, fileNumber);
        BSONObject bsonObject = dataRef.getBsonObject();
        Set<String> bsonKeys = bsonObject.keySet();
        Preconditions.checkArgument(bsonKeys.size() == 1);
        if (key.equals(getKey(bsonObject)))
            return bsonObject;
        return readFrom(key, dataRef.getNextPointer(), dataRef.getNextFileNumber());
    }

    protected String getKey(BSONObject bsonObject) {
        Set<String> bsonKeys = bsonObject.keySet();
        Preconditions.checkArgument(bsonKeys.size() == 1);
        for (String bsonKey : bsonKeys)
            return bsonKey;
        throw new IllegalArgumentException("Specified bsonObject is not key-value forms.");
    }

    protected DataRef getDataRef(int dataPos, byte fileNumber) throws IOException {
        RandomAccessFile dataFile = getDataFile(fileNumber);
        dataFile.seek(dataPos);
        int dataLength = dataFile.readInt();
        int bodySize = dataLength - NEXT_DATA_POINTER_SIZE;
        byte[] buf = new byte[bodySize];
        if (dataFile.read(buf) < bodySize)
            throw new RuntimeException("error");
        BSONObject bsonObject = decoder.readObject(buf);
        int nextDataPos = dataFile.readInt();
        byte nextFileNumber = dataFile.readByte();
        return new DataRef(bsonObject, dataPos, fileNumber, nextDataPos, nextFileNumber);
    }

    public boolean isEmpty() {
        throw new NotImplementedException();
    }

    public Set<String> keySet() {
        throw new NotImplementedException();
    }

    public V remove(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        return removeInternal(key);
    }

    protected V removeInternal(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("removeInternal, key:{}", key);
        }
        long hashCode = toUnsignedInt(key.hashCode());
        int idx = (int) (hashCode / INDEX_SIZE_PER_FILE) + 1;
        int pos = (int) (hashCode % INDEX_SIZE_PER_FILE);

        RandomAccessFile indexFile = null;
        try {
            indexFile = getIndexFile(idx);
            if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
                return null;
            indexFile.seek(pos);
            int dataPos = indexFile.readInt();
            byte dataFileNumber = indexFile.readByte();
            BSONObject bsonObject = removeBSON(key.toString(), dataPos, dataFileNumber, indexFile, pos,
                                               new ArrayList<DataRef>());
            return rebuildValue(bsonObject);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected BSONObject removeBSON(String key, int dataPos, byte dataFileNumber, RandomAccessFile indexFile,
                                    int indexPos, List<DataRef> dataRefList) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("removeBSON, key:{}, dataPos:{}, dataFileNumber:{}, indexFile:{}, indexPos:{}, dataRefList:{}",
                         new Object[] { key, dataPos, dataFileNumber, indexFile, indexPos, dataRefList });
        }
        DataRef dataRef = getDataRef(dataPos, dataFileNumber);
        if (key.equals(getKey(dataRef.getBsonObject()))) {
            if ((dataRef.getNextPointer() == 0) && (dataRef.getNextFileNumber() == 0) && (dataRefList.size() == 0)) {
                // remain this one only
                clearIndex(indexFile, indexPos);
            } else if (dataRefList.size() > 0) {
                DataRef lastDataRef = dataRefList.get(dataRefList.size() - 1);
                RandomAccessFile dataFile = getDataFile(lastDataRef.getCurrentFileNumber());
                dataFile.seek(lastDataRef.getCurrentPointer());
                int length = dataFile.readInt();
                dataFile.seek(lastDataRef.getCurrentPointer() + DATA_LENGTH_FIELD_SIZE + length -
                              NEXT_DATA_POINTER_SIZE);
                dataFile.writeInt(dataRef.getNextPointer());
                dataFile.writeByte(dataRef.getNextFileNumber());
            } else {
                // remove at the first element.
                updateIndex(indexFile, indexPos, dataRef.getNextPointer(), dataRef.getNextFileNumber());
            }
            return dataRef.getBsonObject();
        } else {
            dataRefList.add(dataRef);
            return removeBSON(key, dataRef.getNextPointer(), dataRef.getNextFileNumber(), indexFile, indexPos,
                              dataRefList);
        }
    }

    protected void clearIndex(RandomAccessFile indexFile, int pos) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("clearIndex, indexFile:%s, pos:%d", indexFile, pos);
        }
        indexFile.seek(pos);
        indexFile.writeInt(0);
        indexFile.writeByte(0);
    }

    protected void updateIndex(RandomAccessFile indexFile, int indexPos, int dataPos, byte dataFileNumber)
            throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("updateIndex, indexPos:%d, dataPos:%d, dataFileNumber:%d", new Object[] { indexPos, dataPos,
                                                                                                  dataFileNumber });
        }
        indexFile.seek(indexPos);
        indexFile.writeInt(dataPos);
        indexFile.writeByte(dataFileNumber);
        if (logger.isTraceEnabled()) {
            logger.trace("\twrite to index file, indexPos:%d, dataPos:%d, dataFileNumber:%d",
                         new Object[] { indexPos, dataPos, dataFileNumber });
        }
    }

    public V put(String key, V value) {
        if (logger.isTraceEnabled()) {
            logger.trace("put, key:%s, value:%s", key, value);
        }
        V pre = getInternal(key);
        if (pre != null) {
            removeInternal(key);
        }

        long hashCode = toUnsignedInt(key.hashCode());
        int idx = (int) (hashCode / INDEX_SIZE_PER_FILE) + 1;
        int pos = (int) (hashCode % INDEX_SIZE_PER_FILE);

        RandomAccessFile indexFile = null;
        try {
            indexFile = getIndexFile(idx);
            indexFile.seek(pos);
            writeTo(indexFile, key, value);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return pre;
    }

    protected boolean containsKey(RandomAccessFile indexFile, int pos) throws IOException {
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return false;
        indexFile.seek(pos + 4/* size of dataPos */);
        int fileNumber = indexFile.readByte();
        return (fileNumber > 0);
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
        if (logger.isTraceEnabled()) {
            logger.trace("writeTo, indexFile:%s, bytes:%s", indexFile, bytes);
        }
        RandomAccessFile dataFile = null;
        try {
            byte lastDataFileNumber = getLastDataFileNumber();
            dataFile = getDataFile(lastDataFileNumber);

            long dataPos = dataFile.length();
            dataFile.seek(dataPos);
            int length = bytes.length + NEXT_DATA_POINTER_SIZE;
            dataFile.writeInt(length);
            dataFile.write(bytes);
            dataFile.writeInt(0); // the file position of next data.
            dataFile.writeByte(0); // the file position of next data.

            if (logger.isTraceEnabled()) {
                logger.trace("\twrite to data file, dataPos:%d, length:%d", dataPos, DATA_LENGTH_FIELD_SIZE + length);
            }
            updateIndex(indexFile, dataPos, lastDataFileNumber);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void updateIndex(RandomAccessFile indexFile, long dataPos, byte lastDataFileNumber) throws IOException {
        int dataRef = 0;
        byte dataFileNumber = 0;

        boolean indexWritable = false;
        long pos = indexFile.getFilePointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD) {
            indexWritable = true;
        } else {
            dataRef = indexFile.readInt();
            dataFileNumber = indexFile.readByte();
            if ((dataRef == 0) && (dataFileNumber == 0)) {
                indexWritable = true;
            }
        }
        if (indexWritable) {
            updateIndex(indexFile, (int) pos, (int) dataPos, lastDataFileNumber);
        } else {
            updateNextRef((int) dataPos, lastDataFileNumber, dataRef, dataFileNumber);
        }
    }

    protected void updateNextRef(int orgDataPos, byte orgDataFileNumber, int pos, byte fileNumber) throws IOException {
        int dataPos = pos;
        byte dataFileNumber = fileNumber;

        while (true) {
            RandomAccessFile f = getDataFile(dataFileNumber);
            f.seek(dataPos);
            int dataSize = f.readInt();
            f.seek(dataPos + DATA_LENGTH_FIELD_SIZE + dataSize - NEXT_DATA_POINTER_SIZE);
            int nextDataPos = f.readInt();
            byte nextFileNumber = f.readByte();
            if (nextDataPos == 0 && nextFileNumber == 0) {
                f.seek(dataPos + DATA_LENGTH_FIELD_SIZE + dataSize - NEXT_DATA_POINTER_SIZE);
                f.writeInt(orgDataPos);
                f.writeByte(orgDataFileNumber);
                if (logger.isTraceEnabled()) {
                    logger.trace("\tupdate to data file, dataPos:%d, nextDataPos:%d, nextDataFileNumber:%d",
                                 new Object[] { dataPos, orgDataPos, orgDataFileNumber });
                }
                return;
            } else {
                dataPos = nextDataPos;
                dataFileNumber = nextFileNumber;
            }
        }
    }

    protected byte[] toByteArray(String key, V v) throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        BSONObject object = objectMapper.createBSONObject(key, v);
        return encoder.encode(object);
    }

    @SuppressWarnings("unchecked")
    protected V rebuildValue(BSONObject object) {
        try {
            Set<String> keySet = object.keySet();
            Preconditions.checkArgument(keySet.size() == 1);

            for (String key : keySet) {
                Object target = object.get(key);
                Object v = null;
                if (target instanceof BSONObject) {
                    v = objectMapper.toObject((BSONObject) target);
                } else {
                    v = target;
                }
                return (V) v;
            }
        } catch (IllegalAccessException ex) {
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

    protected byte getLastDataFileNumber() {
        byte i = MAX_NUMBER_OF_DATA_FILES;
        while (true) {
            String dataFilePath = dirPath + File.separator + Integer.toString(i) + DATA_FILE_SUFFIX;
            if (!new File(dataFilePath).exists())
                return --i;
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

    public void close() {
        close(dataFiles);
        close(indexFiles);
    }

    protected void close(RandomAccessFile[] files) {
        for (RandomAccessFile file : files) {
            Closeables.closeQuietly(file);
        }
    }

    protected RandomAccessFile getDataFile(byte dataFileNumber) throws FileNotFoundException {
        Preconditions.checkArgument(dataFileNumber <= MAX_NUMBER_OF_DATA_FILES);
        int idx = dataFileNumber - 1;
        if (dataFiles[idx] != null)
            return dataFiles[idx];
        String dataFilePath = getDataFilePath(dataFileNumber);
        RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
        dataFiles[idx] = dataFile;
        return dataFile;
    }

    protected RandomAccessFile getIndexFile(int indexFileNumber) throws FileNotFoundException {
        Preconditions.checkArgument(indexFileNumber <= MAX_NUMBER_OF_INDEX_FILES);
        int idx = indexFileNumber - 1;
        if (indexFiles[idx] != null)
            return indexFiles[idx];
        String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
        RandomAccessFile indexFile = new RandomAccessFile(indexFilePath, "rw");
        indexFiles[idx] = indexFile;
        return indexFile;
    }

    protected long toUnsignedInt(int i) {
        return i & 0xffffffffL;
    }

    protected void dump(byte[] bin) {
        StringBuilder buf = new StringBuilder();
        for (byte b : bin) {
            buf.append(Integer.toHexString(b & 0xFF) + " ");
        }
        logger.debug(buf.toString());
    }
}
