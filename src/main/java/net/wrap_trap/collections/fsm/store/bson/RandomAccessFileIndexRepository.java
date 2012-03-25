package net.wrap_trap.collections.fsm.store.bson;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import net.wrap_trap.collections.fsm.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

/**
 * <pre>
 *  structure of index
 * +--+--+--+--+--+--+--+--+--+
 * |           a.          |b.|
 * +--+-+--+--+--+---+--+--+--+
 * 
 * a. a offset of data file.[long]
 * b. data file number(1-2).[byte]
 * </pre>
 */
public class RandomAccessFileIndexRepository implements IndexRepository {

    private static final int INDEX_SIZE_PER_RECORD = 9;
    private static final int HEADER_SIZE = 128;

    private static final int VERSION = 1;

    private static final String INDEX_FILE_SUFFIX = ".idx";
    private static final int HEADER_ENTRYCOUNT_OFFSET = 8;

    protected static Logger logger = LoggerFactory.getLogger(RandomAccessFileIndexRepository.class);

    private int currentVersion;

    private RandomAccessFile indexFile;

    private Configuration configuration;

    public RandomAccessFileIndexRepository(Configuration configuration) throws IOException {
        this.configuration = configuration;
        loadIndexFile();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#getIndexRef(
     * long)
     */
    @Override
    public BsonDataBlockPosition getIndexRef(long hashCode) {
        long pos = ((hashCode % configuration.getBucketSize()) * INDEX_SIZE_PER_RECORD) + HEADER_SIZE;
        return new BsonDataBlockPosition((byte) 1, pos);
    }

    protected RandomAccessFile loadIndexFile() throws IOException {
        if (indexFile != null) {
            return indexFile;
        }

        String indexFilePath = configuration.getDirPath() + File.separator + "1" + INDEX_FILE_SUFFIX;
        File file = new File(indexFilePath);
        boolean isNew = !file.exists();

        indexFile = new RandomAccessFile(file, "rw");
        if (isNew) {
            initializeIndexfile(indexFile);
        } else {
            loadHeader(indexFile);
        }
        return indexFile;
    }

    protected void loadHeader(RandomAccessFile indexFile) throws IOException {
        this.currentVersion = indexFile.readInt();

        int loadedBucketSize = indexFile.readInt();
        int configBucketSize = this.configuration.getBucketSize();
        if (configBucketSize != loadedBucketSize) {
            logger.warn("Specified bucketSize '{}' is different from the bucketSize '{}' in the header of index file.",
                        configBucketSize, loadedBucketSize);
            logger.warn("Specified bucketSize '{}' is ignored.", configBucketSize);
        }
        this.configuration.setBucketSize(loadedBucketSize);

        long loadedDataFileSize = indexFile.readLong();
        long configDataFileSize = configuration.getDataFileSize();
        if (configDataFileSize != loadedDataFileSize) {
            logger.warn("Specified dataFileSize '{}' is different from the dataFileSize '{}' in the header of index file.",
                        configDataFileSize, loadedDataFileSize);
            logger.warn("Specified dataFileSize '{}' is ignored.", configDataFileSize);
        }
        this.configuration.setDataFileSize(loadedDataFileSize);
    }

    protected void initializeIndexfile(RandomAccessFile indexFile) throws IOException {
        this.currentVersion = VERSION;
        indexFile.writeInt(this.currentVersion);
        indexFile.writeInt(this.configuration.getBucketSize());
        indexFile.writeLong(this.configuration.getDataFileSize());
        indexFile.setLength(HEADER_SIZE + (INDEX_SIZE_PER_RECORD * this.configuration.getBucketSize()));
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#getDataPosition
     * (java.io.RandomAccessFile,
     * net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public BsonDataBlockPosition getDataPosition(BsonDataBlockPosition indexRef) throws IOException {
        long pos = indexRef.getPointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return null;
        indexFile.seek(pos);
        return read();
    }

    @Override
    public void close() {
        Closeables.closeQuietly(indexFile);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#updateIndex(
     * net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition,
     * net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public void updateIndex(BsonDataBlockPosition indexRef, BsonDataBlockPosition dataRef) throws IOException {
        long indexPos = indexRef.getPointer();
        long dataPos = dataRef.getPointer();
        byte dataFileNumber = dataRef.getFileNumber();

        if (logger.isTraceEnabled()) {
            logger.trace("updateIndex, indexPos:{}, dataPos:{}, dataFileNumber:{}", new Object[] { indexPos, dataPos,
                                                                                                  dataFileNumber });
        }
        indexFile.seek(indexPos);
        indexFile.writeLong(dataPos);
        indexFile.writeByte(dataFileNumber);
        if (logger.isTraceEnabled()) {
            logger.trace("\twrite to index file, indexPos:{}, dataPos:{}, dataFileNumber:{}",
                         new Object[] { indexPos, dataPos, dataFileNumber });
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#indexUpdatable
     * (net.wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public boolean indexUpdatable(BsonDataBlockPosition indexRef) throws IOException {
        indexFile.seek(indexRef.getPointer());

        long pos = indexFile.getFilePointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD) {
            return true;
        } else {
            long dataRef = indexFile.readLong();
            byte dataFileNumber = indexFile.readByte();
            BsonDataBlockPosition tmpPos = new BsonDataBlockPosition(dataFileNumber, dataRef);
            if (tmpPos.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#clearIndex(net
     * .wrap_trap.collections.fsm.store.bson.BsonDataBlockPosition)
     */
    @Override
    public void clearIndex(BsonDataBlockPosition indexRef) throws IOException {
        long pos = indexRef.getPointer();
        if (logger.isTraceEnabled()) {
            logger.trace("clearIndex, pos:{}", pos);
        }
        indexFile.seek(pos);
        indexFile.writeLong(0L);
        indexFile.writeByte(0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#setEntryCount
     * (int)
     */
    @Override
    public void setEntryCount(int count) throws IOException {
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        indexFile.writeInt(count);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#getEntryCount()
     */
    @Override
    public int getEntryCount() throws IOException {
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        return indexFile.readInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.wrap_trap.collections.fsm.store.bson.IndexRepository#seekIndexHead()
     */
    @Override
    public void seekIndexHead() throws IOException {
        indexFile.seek(HEADER_SIZE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.wrap_trap.collections.fsm.store.bson.IndexRepository#hasNext()
     */
    @Override
    public boolean hasNext() throws IOException {
        return (indexFile.getFilePointer() < (HEADER_SIZE + (this.configuration.getBucketSize() * INDEX_SIZE_PER_RECORD)));
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.wrap_trap.collections.fsm.store.bson.IndexRepository#read()
     */
    @Override
    public BsonDataBlockPosition read() throws IOException {
        long pos = indexFile.readLong();
        byte fileNumber = indexFile.readByte();
        BsonDataBlockPosition ret = new BsonDataBlockPosition(fileNumber, pos);
        if (!ret.isEmpty()) {
            return ret;
        }
        return null;
    }
}
