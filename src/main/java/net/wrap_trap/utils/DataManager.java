package net.wrap_trap.utils;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import net.wrap_trap.monganez.BSONObjectMapper;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * <pre>
 *  structure of data
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
public class DataManager<V> implements Closeable {

    private static final String DATA_FILE_SUFFIX = ".dat";

    private static final int MAX_NUMBER_OF_DATA_FILES = 2;
    public static final int DATA_LENGTH_FIELD_SIZE = 4; // (a.) size of integer.
    public static final int NEXT_DATA_POINTER_SIZE = 5;

    protected static Logger logger = LoggerFactory.getLogger(DataManager.class);

    private RandomAccessFile[] dataFiles = new RandomAccessFile[MAX_NUMBER_OF_DATA_FILES];

    private BasicBSONEncoder encoder = new BasicBSONEncoder();
    private BasicBSONDecoder decoder = new BasicBSONDecoder();
    private BSONObjectMapper objectMapper = new BSONObjectMapper();
    private String dirPath;

    public DataManager(String path) {
        this.dirPath = path;
    }

    public BSONObject readFrom(String key, int dataPos, byte fileNumber) throws IOException, FileNotFoundException {
        if (logger.isTraceEnabled()) {
            logger.trace("readFrom, key:{}, dataPos:{}, fileNumber:{}", new Object[] { key, dataPos, fileNumber });
        }
        if (dataPos == 0 && fileNumber == 0)
            // index record is empty.(this record area has cleaned up.)
            return null;
        return readDataFile(key, dataPos, fileNumber);
    }

    public void updateNextRef(DataBlock dataRef, DataBlock lastDataRef) throws IOException {
        RandomAccessFile dataFile = getDataFile(lastDataRef.getCurrentFileNumber());
        dataFile.seek(lastDataRef.getCurrentPointer());
        int length = dataFile.readInt();
        dataFile.seek(lastDataRef.getCurrentPointer() + DataManager.DATA_LENGTH_FIELD_SIZE + length -
                      DataManager.NEXT_DATA_POINTER_SIZE);
        dataFile.writeInt(dataRef.getNextPointer());
        dataFile.writeByte(dataRef.getNextFileNumber());
    }

    public void updateNextRef(int orgDataPos, byte orgDataFileNumber, int pos, byte fileNumber) throws IOException {
        int dataPos = pos;
        byte dataFileNumber = fileNumber;

        while (true) {
            RandomAccessFile f = getDataFile(dataFileNumber);
            f.seek(dataPos);
            int dataSize = f.readInt();
            f.seek(dataPos + DataManager.DATA_LENGTH_FIELD_SIZE + dataSize - DataManager.NEXT_DATA_POINTER_SIZE);
            int nextDataPos = f.readInt();
            byte nextFileNumber = f.readByte();
            if (nextDataPos == 0 && nextFileNumber == 0) {
                f.seek(dataPos + DataManager.DATA_LENGTH_FIELD_SIZE + dataSize - DataManager.NEXT_DATA_POINTER_SIZE);
                f.writeInt(orgDataPos);
                f.writeByte(orgDataFileNumber);
                if (logger.isTraceEnabled()) {
                    logger.trace("\tupdate to data file, dataPos:{}, nextDataPos:{}, nextDataFileNumber:{}",
                                 new Object[] { dataPos, orgDataPos, orgDataFileNumber });
                }
                return;
            } else {
                dataPos = nextDataPos;
                dataFileNumber = nextFileNumber;
            }
        }
    }

    @Override
    public void close() {
        for (RandomAccessFile file : dataFiles) {
            Closeables.closeQuietly(file);
        }
    }

    protected BSONObject readDataFile(String key, int dataPos, byte fileNumber) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("readDataFile, key:{}, dataPos:{}, fileNumber:{}", new Object[] { key, dataPos, fileNumber });
        }
        DataBlock dataBlock = getDataBlock(dataPos, fileNumber);
        if (key.equals(getKey(dataBlock)))
            return dataBlock.getBsonObject();
        return readFrom(key, dataBlock.getNextPointer(), dataBlock.getNextFileNumber());
    }

    public DataBlock getDataBlock(int dataPos, byte fileNumber) throws IOException {
        RandomAccessFile dataFile = getDataFile(fileNumber);
        dataFile.seek(dataPos);
        int dataLength = dataFile.readInt();
        int bodySize = dataLength - DataManager.NEXT_DATA_POINTER_SIZE;
        byte[] buf = new byte[bodySize];
        if (dataFile.read(buf) < bodySize)
            throw new RuntimeException("error");
        BSONObject bsonObject = decoder.readObject(buf);
        int nextDataPos = dataFile.readInt();
        byte nextFileNumber = dataFile.readByte();
        return new DataBlock(bsonObject, dataPos, fileNumber, nextDataPos, nextFileNumber);
    }

    public String getKey(DataBlock dataBlock) {
        Set<String> bsonKeys = dataBlock.getBsonObject().keySet();
        Preconditions.checkArgument(bsonKeys.size() == 1);
        for (String bsonKey : bsonKeys)
            return bsonKey;
        throw new IllegalArgumentException("Specified bsonObject is not key-value forms.");
    }

    protected String getDataFilePath(byte fileNumber) {
        return dirPath + File.separator + Integer.toString(fileNumber) + DATA_FILE_SUFFIX;
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

    public Position writeTo(RandomAccessFile indexFile, String key, V value) {
        try {
            byte[] bytes = toByteArray(key, value);
            return writeTo(indexFile, bytes);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected Position writeTo(RandomAccessFile indexFile, byte[] bytes) {
        if (logger.isTraceEnabled()) {
            logger.trace("writeTo, indexFile:{}, bytes:{}", indexFile, bytes);
        }
        RandomAccessFile dataFile = null;
        try {
            byte lastDataFileNumber = getLastDataFileNumber();
            dataFile = getDataFile(lastDataFileNumber);

            long dataPos = dataFile.length();
            dataFile.seek(dataPos);
            int length = bytes.length + DataManager.NEXT_DATA_POINTER_SIZE;
            dataFile.writeInt(length);
            dataFile.write(bytes);
            dataFile.writeInt(0); // the file position of next data.
            dataFile.writeByte(0); // the file position of next data.

            if (logger.isTraceEnabled()) {
                logger.trace("\twrite to data file, dataPos:{}, length:{}", dataPos,
                             DataManager.DATA_LENGTH_FIELD_SIZE + length);
            }
            return new Position(lastDataFileNumber, (int) dataPos);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
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

}
