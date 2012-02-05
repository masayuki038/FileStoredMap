package net.wrap_trap.utils;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
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

    private static final String INDEX_FILE_SUFFIX = ".idx";
    private static final int INDEX_SIZE_PER_RECORD = 5;
    private static final int MAX_NUMBER_OF_INDEX_FILES = 10; // Integer.MAX_VALUE(2^31)/INDEX_SIZE_PER_FILE*2(negative/positive areas of integer)
    private static final int INDEX_SIZE_PER_FILE = 429496729; // Integer.MAX_VALUE(2^31-1)/5 4byte:index file position, 1byte: file index.

    protected static Logger logger = LoggerFactory.getLogger(Indexer.class);

    private RandomAccessFile[] indexFiles = new RandomAccessFile[MAX_NUMBER_OF_INDEX_FILES];

    private String dirPath;

    public Indexer(String path) {
        super();
        this.dirPath = path;
    }

    public Position getIndexRef(String key) {
        long hashCode = toUnsignedInt(key.hashCode());
        byte idx = (byte) ((hashCode / (INDEX_SIZE_PER_FILE * INDEX_SIZE_PER_RECORD)) + 1);
        int pos = (int) ((hashCode % INDEX_SIZE_PER_FILE) * INDEX_SIZE_PER_RECORD);
        return new Position(idx, pos);
    }

    public RandomAccessFile getIndexFile(byte indexFileNumber) throws FileNotFoundException {
        Preconditions.checkArgument(indexFileNumber <= MAX_NUMBER_OF_INDEX_FILES);
        int idx = indexFileNumber - 1;
        if (indexFiles[idx] != null)
            return indexFiles[idx];
        String indexFilePath = dirPath + File.separator + Integer.toString(idx) + INDEX_FILE_SUFFIX;
        RandomAccessFile indexFile = new RandomAccessFile(indexFilePath, "rw");
        indexFiles[idx] = indexFile;
        return indexFile;
    }

    public Position getDataPosition(RandomAccessFile indexFile, Position indexRef) throws IOException {
        int pos = indexRef.getPointer();
        if (indexFile.length() < pos + INDEX_SIZE_PER_RECORD)
            return null;
        indexFile.seek(pos);
        int dataPos = indexFile.readInt();
        byte dataFileNumber = indexFile.readByte();
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

    protected long toUnsignedInt(int i) {
        return i & 0xffffffffL;
    }

}
