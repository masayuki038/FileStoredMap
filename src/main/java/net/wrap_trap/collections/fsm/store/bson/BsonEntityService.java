package net.wrap_trap.collections.fsm.store.bson;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.wrap_trap.collections.fsm.Configuration;
import net.wrap_trap.monganez.BSONObjectMapper;

import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * <pre>
 *  structure of data
 * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 * |     a.    |     b.    |           c.          |d.|  
 * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 * 
 * a. a data length(from b. to d.)[integer]
 * b. data[byte[]]
 * c. a file position of next data.[long]
 * d. a file number of next data.[byte]
 * </pre>
 */
public class BsonEntityService<V> implements Closeable {

    private static final String DATA_FILE_SUFFIX = ".dat";

    public static final int DATA_LENGTH_FIELD_SIZE = 4; // (a.) size of integer.
    public static final int NEXT_DATA_POINTER_SIZE = 9;

    protected static Logger logger = LoggerFactory.getLogger(BsonEntityService.class);

    private List<RandomAccessFile> dataFileList = new ArrayList<RandomAccessFile>();

    private BSONEncoder encoder = new BSONEncoder();
    private BSONDecoder decoder = new BSONDecoder();
    private BSONObjectMapper objectMapper = new BSONObjectMapper();
    private Configuration configuration;

    public BsonEntityService(Configuration configuration) {
        this.configuration = configuration;
    }

    public BSONObject readFrom(String key, BsonDataBlockPosition dataRef) throws IOException, FileNotFoundException {
        if (logger.isTraceEnabled()) {
            logger.trace("readFrom, key:{}, dataRef:{}", new Object[] { key, dataRef });
        }
        if (dataRef.isEmpty())
            // index record is empty.(this record area has cleaned up.)
            return null;
        return readDataFile(key, dataRef);
    }

    public void updateNextRef(BsonDataBlock dataRef, BsonDataBlock lastDataRef) throws IOException {
        RandomAccessFile dataFile = getDataFile(lastDataRef.getCurrentFileNumber());
        dataFile.seek(lastDataRef.getCurrentPointer());
        int length = dataFile.readInt();
        dataFile.seek(lastDataRef.getCurrentPointer() + BsonEntityService.DATA_LENGTH_FIELD_SIZE + length -
                      BsonEntityService.NEXT_DATA_POINTER_SIZE);
        dataFile.writeLong(dataRef.getNextPointer());
        dataFile.writeByte(dataRef.getNextFileNumber());
    }

    public void updateNextRef(BsonDataBlockPosition rootDataRef, BsonDataBlockPosition nextDataRef) throws IOException {
        long dataPos = rootDataRef.getPointer();
        byte dataFileNumber = rootDataRef.getFileNumber();

        while (true) {
            RandomAccessFile f = getDataFile(dataFileNumber);
            Preconditions.checkArgument((dataPos >= 0L), "dataPos < 0 dataPos: %d", dataPos);
            f.seek(dataPos);
            int dataSize = f.readInt();
            f.seek(dataPos + BsonEntityService.DATA_LENGTH_FIELD_SIZE + dataSize -
                   BsonEntityService.NEXT_DATA_POINTER_SIZE);
            long nextDataPos = f.readLong();
            byte nextFileNumber = f.readByte();
            BsonDataBlockPosition tmpRef = new BsonDataBlockPosition(nextFileNumber, nextDataPos);
            if (tmpRef.isEmpty()) {
                f.seek(dataPos + BsonEntityService.DATA_LENGTH_FIELD_SIZE + dataSize -
                       BsonEntityService.NEXT_DATA_POINTER_SIZE);
                f.writeLong(nextDataRef.getPointer());
                f.writeByte(nextDataRef.getFileNumber());
                if (logger.isTraceEnabled()) {
                    logger.trace("\tupdate to data file, dataPos:{}, nextDataPos:{}, nextDataFileNumber:{}",
                                 new Object[] { dataPos, nextDataRef.getPointer(), nextDataRef.getFileNumber() });
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
        for (RandomAccessFile file : dataFileList) {
            Closeables.closeQuietly(file);
        }
    }

    protected BSONObject readDataFile(String key, BsonDataBlockPosition dataRef) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("readDataFile, key:{}, dataRef:{}", new Object[] { key, dataRef });
        }
        BsonDataBlock bsonDataBlock = getDataBlock(dataRef);
        if (key.equals(getKey(bsonDataBlock)))
            return bsonDataBlock.getBsonObject();
        return readFrom(key,
                        new BsonDataBlockPosition(bsonDataBlock.getNextFileNumber(), bsonDataBlock.getNextPointer()));
    }

    public BsonDataBlock getDataBlock(BsonDataBlockPosition dataRef) throws IOException {
        byte fileNumber = dataRef.getFileNumber();
        long dataPos = dataRef.getPointer();

        RandomAccessFile dataFile = getDataFile(fileNumber);
        dataFile.seek(dataPos);
        int dataLength = dataFile.readInt();
        int bodySize = dataLength - BsonEntityService.NEXT_DATA_POINTER_SIZE;
        byte[] buf = new byte[bodySize];
        int read = dataFile.read(buf);
        Preconditions.checkState((read == bodySize), "failed to read data body.");

        BSONObject bsonObject = decoder.readObject(buf);
        long nextDataPos = dataFile.readLong();
        byte nextFileNumber = dataFile.readByte();
        return new BsonDataBlock(bsonObject, dataPos, fileNumber, nextDataPos, nextFileNumber);
    }

    public String getKey(BsonDataBlock bsonDataBlock) {
        Set<String> bsonKeys = bsonDataBlock.getBsonObject().keySet();
        Preconditions.checkArgument(bsonKeys.size() == 1);
        for (String bsonKey : bsonKeys)
            return bsonKey;
        throw new IllegalArgumentException("Specified bsonObject is not key-value forms.");
    }

    public BsonDataBlockPosition writeTo(String key, V value) throws IOException {
        try {
            byte[] bytes = toByteArray(key, value);
            return writeTo(bytes);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected String getDataFilePath(byte fileNumber) {
        return configuration.getDirPath() + File.separator + Integer.toString(fileNumber) + DATA_FILE_SUFFIX;
    }

    protected RandomAccessFile getDataFile(byte dataFileNumber) throws FileNotFoundException {
        //Preconditions.checkArgument();
        int idx = dataFileNumber - 1;
        if (dataFileNumber <= dataFileList.size())
            return dataFileList.get(idx);
        String dataFilePath = getDataFilePath(dataFileNumber);
        RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
        dataFileList.add(idx, dataFile);
        return dataFile;
    }

    protected BsonDataBlockPosition writeTo(byte[] bytes) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("writeTo, bytes:{}", bytes);
        }
        int length = bytes.length + BsonEntityService.NEXT_DATA_POINTER_SIZE;

        byte lastDataFileNumber = getLastDataFileNumber();
        RandomAccessFile dataFile = getDataFile(lastDataFileNumber);

        long dataPos = dataFile.length();
        if ((dataPos + 4/* size of length field */+ length) > configuration.getDataFileSize()) {
            dataFile = getDataFile(++lastDataFileNumber);
            dataPos = dataFile.length();
        }
        dataFile.seek(dataPos);

        dataFile.writeInt(length);
        dataFile.write(bytes);
        dataFile.writeLong(0L); // the file position of next data.
        dataFile.writeByte(0); // the file position of next data.

        if (logger.isTraceEnabled()) {
            logger.trace("\twrite to data file, dataPos:{}, length:{}", dataPos,
                         BsonEntityService.DATA_LENGTH_FIELD_SIZE + length);
        }
        return new BsonDataBlockPosition(lastDataFileNumber, dataPos);
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
        byte i = 1;
        while (true) {
            String dataFilePath = configuration.getDirPath() + File.separator + Integer.toString(i) + DATA_FILE_SUFFIX;
            if (!new File(dataFilePath).exists()) {
                return (byte) i;
            } else {
                i++;
            }
        }
    }
}
