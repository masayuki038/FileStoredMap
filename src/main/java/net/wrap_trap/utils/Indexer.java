package net.wrap_trap.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * <pre>
 *  structure of index
 * +--+--+--+--+--+
 * |     a.    |b.|
 * +--+--+--+--+--+
 * 
 * a. a offset of data file.[integer]
 * b. data file number(1-2).[byte]
 * </pre>
 */
public class Indexer implements Closeable {

    private static final int VERSION = 1;

    private static final String INDEX_FILE_SUFFIX = ".idx";
    private static final int INDEX_SIZE_PER_RECORD = 5;
    private static final int MAX_NUMBER_OF_INDEX_FILES = 10; // Integer.MAX_VALUE(2^31)/INDEX_SIZE_PER_FILE*2(negative/positive areas of integer)
    private static final int HEADER_ENTRYCOUNT_OFFSET = 8;

    private static final int HEADER_SIZE = 128;

    protected static Logger logger = LoggerFactory.getLogger(Indexer.class);

    private RandomAccessFile[] indexFiles = new RandomAccessFile[MAX_NUMBER_OF_INDEX_FILES];

    private String dirPath;
    private int bucketSize;
    private int currentVersion;

    public Indexer(String path) {
        this(path, 4096);
    }

    public Indexer(String path, int bucketSize) {
        super();
        this.dirPath = path;
        this.bucketSize = bucketSize;
    }

    public Position getIndexRef(String key) {
        long hashCode = toUnsignedInt(key.hashCode());
        byte idx = 1;
        int pos = (int) ((hashCode % bucketSize) * INDEX_SIZE_PER_RECORD) + HEADER_SIZE;
        return new Position(idx, pos);
    }

    public RandomAccessFile getIndexFile(byte indexFileNumber) throws IOException {
        Preconditions.checkArgument(indexFileNumber <= MAX_NUMBER_OF_INDEX_FILES);
        int idx = indexFileNumber - 1;
        if (indexFiles[idx] != null)
            return indexFiles[idx];
        String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;

        File file = new File(indexFilePath);
        boolean isNew = !file.exists();

        RandomAccessFile indexFile = new RandomAccessFile(file, "rw");
        if (isNew) {
            initializeIndexfile(indexFile);
        } else {
            loadHeader(indexFile);
        }
        indexFiles[idx] = indexFile;
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

    public Position getDataPosition(RandomAccessFile indexFile, Position indexRef) throws IOException {
        int pos = indexRef.getPointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return null;
        indexFile.seek(pos);
        int dataPos = indexFile.readInt();
        byte dataFileNumber = indexFile.readByte();
        if (dataFileNumber == 0) {
            return null;
        }
        return new Position(dataFileNumber, dataPos);
    }

    public boolean containsKey(RandomAccessFile indexFile, Position indexRef) throws IOException {
        int pos = indexRef.getPointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return false;
        indexFile.seek(pos + 4/* size of dataPos */);
        int fileNumber = indexFile.readByte();
        return (fileNumber > 0);
    }

    public void updateIndex(RandomAccessFile indexFile, int indexPos, int dataPos, byte dataFileNumber)
            throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("updateIndex, indexPos:{}, dataPos:{}, dataFileNumber:{}", new Object[] { indexPos, dataPos,
                                                                                                  dataFileNumber });
        }
        indexFile.seek(indexPos);
        indexFile.writeInt(dataPos);
        indexFile.writeByte(dataFileNumber);
        if (logger.isTraceEnabled()) {
            logger.trace("\twrite to index file, indexPos:{}, dataPos:{}, dataFileNumber:{}",
                         new Object[] { indexPos, dataPos, dataFileNumber });
        }
    }

    public boolean indexUpdatable(RandomAccessFile indexFile) throws IOException {
        boolean indexWritable = false;
        long pos = indexFile.getFilePointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD) {
            indexWritable = true;
        } else {
            int dataRef = indexFile.readInt();
            byte dataFileNumber = indexFile.readByte();
            if ((dataRef == 0) && (dataFileNumber == 0)) {
                indexWritable = true;
            }
        }
        return indexWritable;
    }

    @Override
    public void close() {
        for (RandomAccessFile file : indexFiles) {
            Closeables.closeQuietly(file);
        }
    }

    public void clearIndex(RandomAccessFile indexFile, int pos) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("clearIndex, indexFile:{}, pos:{}", indexFile, pos);
        }
        indexFile.seek(pos);
        indexFile.writeInt(0);
        indexFile.writeByte(0);
    }

    protected long toUnsignedInt(int i) {
        return i & 0xffffffffL;
    }

    public void incrementEntryCount() throws IOException {
        int count = getEntryCount();
        RandomAccessFile indexFile = getIndexFile((byte) 1);
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        indexFile.writeInt(count + 1);
    }

    public void decrementEntryCount() throws IOException {
        int count = getEntryCount();
        RandomAccessFile indexFile = getIndexFile((byte) 1);
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        indexFile.writeInt(count - 1);
    }

    public int getEntryCount() throws IOException {
        RandomAccessFile indexFile = getIndexFile((byte) 1);
        indexFile.seek(HEADER_ENTRYCOUNT_OFFSET);
        return indexFile.readInt();
    }
}
