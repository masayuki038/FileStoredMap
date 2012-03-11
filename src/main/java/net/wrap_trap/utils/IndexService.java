package net.wrap_trap.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

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
public class IndexService implements Closeable {

    private static final int VERSION = 1;

    private static final String INDEX_FILE_SUFFIX = ".idx";
    private static final int INDEX_SIZE_PER_RECORD = 9;
    private static final int HEADER_ENTRYCOUNT_OFFSET = 8;

    private static final int HEADER_SIZE = 128;

    protected static Logger logger = LoggerFactory.getLogger(IndexService.class);

    private RandomAccessFile indexFile;

    private String dirPath;
    private int bucketSize;
    private int currentVersion;

    public IndexService(String path) {
        this(path, 4096);
    }

    public IndexService(String path, int bucketSize) {
        super();
        this.dirPath = path;
        this.bucketSize = bucketSize;
    }

    public Position getIndexRef(String key) {
        long hashCode = toUnsignedInt(key.hashCode());
        long pos = ((hashCode % bucketSize) * INDEX_SIZE_PER_RECORD) + HEADER_SIZE;
        return new Position((byte) 1, pos);
    }

    protected RandomAccessFile getIndexFile() throws IOException {
        if (indexFile != null) {
            return indexFile;
        }

        String indexFilePath = dirPath + File.separator + "1" + INDEX_FILE_SUFFIX;

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
        if (this.bucketSize != loadedBucketSize) {
            logger.warn("Specified bucketSize '{}' is different from the bucketSize '{}' in the header of index file.",
                        this.bucketSize, loadedBucketSize);
            logger.warn("Specified bucketSize '{}' is ignored.", this.bucketSize);
        }
        this.bucketSize = loadedBucketSize;
    }

    protected void initializeIndexfile(RandomAccessFile indexFile) throws IOException {
        this.currentVersion = VERSION;
        indexFile.writeInt(this.currentVersion);
        indexFile.writeInt(this.bucketSize);
        indexFile.setLength(HEADER_SIZE + (INDEX_SIZE_PER_RECORD * this.bucketSize));
    }

    public Position getDataPosition(String key) throws IOException {
        return getDataPosition(getIndexRef(key));
    }

    public Position getDataPosition(Position indexRef) throws IOException {
        return getDataPosition(getIndexFile(), indexRef);
    }

    protected Position getDataPosition(RandomAccessFile indexFile, Position indexRef) throws IOException {
        long pos = indexRef.getPointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return null;
        indexFile.seek(pos);
        long dataPos = indexFile.readLong();
        byte dataFileNumber = indexFile.readByte();
        if (dataFileNumber == 0) {
            return null;
        }
        return new Position(dataFileNumber, dataPos);
    }

    public boolean containsKey(RandomAccessFile indexFile, Position indexRef) throws IOException {
        long pos = indexRef.getPointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return false;
        indexFile.seek(pos + 4/* size of dataPos */);
        int fileNumber = indexFile.readByte();
        return (fileNumber > 0);
    }

    public void updateIndex(Position indexRef, Position dataRef) throws IOException {
        long indexPos = indexRef.getPointer();
        long dataPos = dataRef.getPointer();
        byte dataFileNumber = dataRef.getFileNumber();

        if (logger.isTraceEnabled()) {
            logger.trace("updateIndex, indexPos:{}, dataPos:{}, dataFileNumber:{}", new Object[] { indexPos, dataPos,
                                                                                                  dataFileNumber });
        }
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(indexPos);
        indexFile.writeLong(dataPos);
        indexFile.writeByte(dataFileNumber);
        if (logger.isTraceEnabled()) {
            logger.trace("\twrite to index file, indexPos:{}, dataPos:{}, dataFileNumber:{}",
                         new Object[] { indexPos, dataPos, dataFileNumber });
        }
    }

    public boolean indexUpdatable(Position indexRef) throws IOException {
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(indexRef.getPointer());

        boolean indexWritable = false;
        long pos = indexFile.getFilePointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD) {
            indexWritable = true;
        } else {
            long dataRef = indexFile.readLong();
            byte dataFileNumber = indexFile.readByte();
            if ((dataRef == 0) && (dataFileNumber == 0)) {
                indexWritable = true;
            }
        }
        return indexWritable;
    }

    @Override
    public void close() {
        Closeables.closeQuietly(indexFile);
    }

    public void clearIndex(Position indexRef) throws IOException {
        long pos = indexRef.getPointer();
        if (logger.isTraceEnabled()) {
            logger.trace("clearIndex, pos:{}", pos);
        }
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(pos);
        indexFile.writeLong(0);
        indexFile.writeByte(0);
    }

    protected long toUnsignedInt(int i) {
        return i & 0xffffffffL;
    }

    public void incrementEntryCount() throws IOException {
        int count = getEntryCount();
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        indexFile.writeInt(count + 1);
    }

    public void decrementEntryCount() throws IOException {
        int count = getEntryCount();
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        indexFile.writeInt(count - 1);
    }

    public int getEntryCount() throws IOException {
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        return indexFile.readInt();
    }

    public void seekIndexHead() throws IOException {
        RandomAccessFile indexFile = getIndexFile();
        indexFile.seek(HEADER_SIZE);
    }

    public boolean hasNext() throws IOException {
        RandomAccessFile indexFile = getIndexFile();
        return (indexFile.getFilePointer() < (HEADER_SIZE + (bucketSize * INDEX_SIZE_PER_RECORD)));
    }

    public Position read() throws IOException {
        RandomAccessFile indexFile = getIndexFile();
        long pos = indexFile.readLong();
        byte fileNumber = indexFile.readByte();
        if (fileNumber > 0) {
            return new Position(fileNumber, pos);
        }
        return null;
    }
}
