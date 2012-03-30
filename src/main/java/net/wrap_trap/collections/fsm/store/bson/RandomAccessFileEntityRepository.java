package net.wrap_trap.collections.fsm.store.bson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import net.wrap_trap.collections.fsm.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * <pre>
 *  data format
 * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 * |     a.    |     b.    |c.|           d.          |  
 * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
 * 
 * a. a data length(from b. to d.)[integer]
 * b. data[byte[]]
 * c. a file number of next data.[byte]
 * d. a file position of next data.[long]
 * </pre>
 */
public class RandomAccessFileEntityRepository implements EntityRepository {

    private static final String DATA_FILE_SUFFIX = ".dat";

    public static final int DATA_LENGTH_FIELD_SIZE = 4; // (a.) size of integer.
    public static final int NEXT_DATA_POINTER_SIZE = 9;

    protected static Logger logger = LoggerFactory.getLogger(RandomAccessFileEntityRepository.class);

    private List<RandomAccessFile> dataFileList = new ArrayList<RandomAccessFile>();

    private Configuration configuration;

    public RandomAccessFileEntityRepository(Configuration configuration) {
        this.configuration = configuration;
    }

    public void close() {
        for (RandomAccessFile file : dataFileList) {
            Closeables.closeQuietly(file);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.EntityRepository#updateDataBlockLink
     * (net.wrap_trap.collections.fsm.store.bson.BsonDataBlock,
     * net.wrap_trap.collections.fsm.store.bson.BsonDataBlock)
     */
    @Override
    public void updateDataBlockLink(BsonDataBlock from, BsonDataBlock to) throws IOException {
        RandomAccessFile dataFile = getDataFile(from.getCurrentFileNumber());
        dataFile.seek(from.getCurrentPointer());
        int length = dataFile.readInt();
        dataFile.seek(from.getCurrentPointer() + DATA_LENGTH_FIELD_SIZE + length - NEXT_DATA_POINTER_SIZE);
        dataFile.writeByte(to.getNextFileNumber());
        dataFile.writeLong(to.getNextPointer());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.EntityRepository#updateDataBlockLink
     * (net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition,
     * net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public void updateDataBlockLink(BsonDataBlockPosition from, BsonDataBlockPosition to) throws FileNotFoundException,
            IOException {
        RandomAccessFile f = getDataFile(from.getFileNumber());
        f.seek(from.getPointer());
        int dataSize = f.readInt();

        f.seek(from.getPointer() + DATA_LENGTH_FIELD_SIZE + dataSize - NEXT_DATA_POINTER_SIZE);
        f.writeByte(to.getFileNumber());
        f.writeLong(to.getPointer());
        if (logger.isTraceEnabled()) {
            logger.trace("\tupdate to data file, dataPos:{}, nextDataPos:{}, nextDataFileNumber:{}",
                         new Object[] { from.getPointer(), to.getPointer(), to.getFileNumber() });
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.wrap_trap.collections.fsm.store.bson.EntityRepository#
     * getLastDataBlockPosition
     * (net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public BsonDataBlockPosition getLastDataBlockPosition(BsonDataBlockPosition start) throws IOException {
        BsonDataBlockPosition current = start;

        while (true) {
            Preconditions.checkArgument((current.getPointer() >= 0L), "dataPos < 0 dataPos: %d", current.getPointer());
            RandomAccessFile f = getDataFile(current.getFileNumber());
            f.seek(current.getPointer());
            int dataSize = f.readInt();
            f.seek(current.getPointer() + DATA_LENGTH_FIELD_SIZE + dataSize - NEXT_DATA_POINTER_SIZE);
            byte nextFileNumber = f.readByte();
            long nextDataPos = f.readLong();
            BsonDataBlockPosition tmpRef = new BsonDataBlockPosition(nextFileNumber, nextDataPos);
            if (tmpRef.isEmpty()) {
                return current;
            } else {
                current = new BsonDataBlockPosition(nextFileNumber, nextDataPos);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.EntityRepository#getDataBlock
     * (net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public BsonDataBlock getDataBlock(BsonDataBlockPosition dataRef) throws IOException {
        byte fileNumber = dataRef.getFileNumber();
        long dataPos = dataRef.getPointer();

        RandomAccessFile dataFile = getDataFile(fileNumber);
        dataFile.seek(dataPos);
        int dataLength = dataFile.readInt();
        int bodySize = dataLength - NEXT_DATA_POINTER_SIZE;
        byte[] buf = new byte[bodySize];
        int read = dataFile.read(buf);
        Preconditions.checkState((read == bodySize), "failed to read data body.");

        byte nextFileNumber = dataFile.readByte();
        long nextDataPos = dataFile.readLong();
        return new BsonDataBlock(buf, dataPos, fileNumber, nextDataPos, nextFileNumber);
    }

    protected String getDataFilePath(byte fileNumber) {
        return configuration.getDirPath() + File.separator + Integer.toString(fileNumber) + DATA_FILE_SUFFIX;
    }

    protected RandomAccessFile getDataFile(byte dataFileNumber) throws FileNotFoundException {
        if (dataFileNumber == 0) {
            // init.
            String dataFilePath = getDataFilePath((byte) 1);
            RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
            dataFileList.add(0, dataFile);
            return dataFile;
        }

        int idx = dataFileNumber - 1;
        if (dataFileNumber <= dataFileList.size())
            return dataFileList.get(idx);
        String dataFilePath = getDataFilePath(dataFileNumber);
        RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
        dataFileList.add(idx, dataFile);
        return dataFile;
    }

    protected RandomAccessFile getLastDataFile() throws FileNotFoundException {
        return getDataFile(getLastDataFileNumber());
    }

    public BsonDataBlockPosition writeTo(byte[] bytes) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("writeTo, bytes:{}", bytes);
        }
        int length = bytes.length + NEXT_DATA_POINTER_SIZE;

        RandomAccessFile dataFile = getLastDataFile();
        byte lastDataFileNumber = getLastDataFileNumber(); // keep the lastFileNumber for returning.
        // FIXME getDataFile() and getLastDataFile() should return the data file with file number for keeping the consistency of lastDataFileNumber.

        long dataPos = dataFile.length();
        if ((dataPos + 4/* size of length field */+ length) > configuration.getDataFileSize()) {
            dataFile = getDataFile((byte) (++lastDataFileNumber));
            dataPos = dataFile.length();
        }
        dataFile.seek(dataPos);

        dataFile.writeInt(length);
        dataFile.write(bytes);
        dataFile.writeByte(0); // the file position of next data.
        dataFile.writeLong(0L); // the file position of next data.

        if (logger.isTraceEnabled()) {
            logger.trace("\twrite to data file, dataPos:{}, length:{}", dataPos, DATA_LENGTH_FIELD_SIZE + length);
        }
        return new BsonDataBlockPosition(lastDataFileNumber, dataPos);
    }

    protected byte getLastDataFileNumber() {
        byte i = 1;
        while (true) {
            String dataFilePath = configuration.getDirPath() + File.separator + Integer.toString(i) + DATA_FILE_SUFFIX;
            if (!new File(dataFilePath).exists()) {
                return (byte) (i - 1);
            } else {
                i++;
            }
        }
    }
}
