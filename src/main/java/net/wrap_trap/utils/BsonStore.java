package net.wrap_trap.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class BsonStore<V> implements Store<V> {

    protected static Logger logger = LoggerFactory.getLogger(BsonStore.class);

    private Indexer indexer;
    private DataManager<V> dataManager;
    private String dirPath;
    private int bucketSize;

    public BsonStore(String path, int bucketSize) {
        this.dirPath = path;
        this.bucketSize = bucketSize;
        initialize();
    }

    @Override
    public V get(String key) {
        if (logger.isTraceEnabled()) {
            logger.trace("get, key:{}", key);
        }
        try {
            Position indexRef = indexer.getIndexRef(key);
            RandomAccessFile indexFile = indexer.getIndexFile(indexRef.getFileNumber());
            if (indexFile == null)
                return null;
            BSONObject bsonObject = readFrom(key.toString(), indexFile, indexRef);
            if (bsonObject == null)
                return null;
            return dataManager.rebuildValue(bsonObject);
        } catch (FileNotFoundException ex) {
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public V put(String key, V value) {
        if (logger.isTraceEnabled()) {
            logger.trace("put, key:{}, value:{}", key, value);
        }
        V pre = get(key);
        if (pre != null) {
            remove(key);
        }

        try {
            synchronized (this) {
                Position indexRef = indexer.getIndexRef(key);
                RandomAccessFile indexFile = indexer.getIndexFile(indexRef.getFileNumber());
                indexFile.seek(indexRef.getPointer());
                Position pos = dataManager.writeTo(indexFile, key, value);
                updateIndex(indexFile, pos.getPointer(), pos.getFileNumber());
                indexer.incrementEntryCount();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return pre;
    }

    @Override
    public V remove(String key) {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        synchronized (this) {
            try {
                Position indexRef = indexer.getIndexRef(key);
                RandomAccessFile indexFile = indexer.getIndexFile(indexRef.getFileNumber());
                Position dataRef = indexer.getDataPosition(indexFile, indexRef);
                if (dataRef == null)
                    return null;
                BSONObject bsonObject = removeBSON(key.toString(), dataRef.getPointer(), dataRef.getFileNumber(),
                                                   indexFile, indexRef.getPointer(), new ArrayList<DataBlock>());
                indexer.decrementEntryCount();
                return dataManager.rebuildValue(bsonObject);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public Set<String> keySet() {
        try {
            final RandomAccessFile indexFile = indexer.getIndexFile((byte) 1);
            indexer.seekIndexHead(indexFile);
            return new LazySet<String>() {
                @Override
                public Iterator<String> iterator() {
                    return new LazyIterator<String>() {

                        Position dataRef;
                        DataBlock dataBlock;

                        {
                            try {
                                dataRef = getNextPosition();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public boolean hasNext() {
                            return (dataRef != null);
                        }

                        @Override
                        public String next() {
                            Preconditions.checkState(hasNext());
                            try {
                                if (this.dataBlock == null) {
                                    dataBlock = dataManager.getDataBlock(dataRef.getPointer(), dataRef.getFileNumber());
                                }
                                String key = dataManager.getKey(dataBlock);
                                if (dataBlock.getNextFileNumber() == 0) {
                                    dataBlock = null;
                                    dataRef = getNextPosition();
                                } else {
                                    dataBlock = dataManager.getDataBlock(dataBlock.getNextPointer(),
                                                                         dataBlock.getNextFileNumber());
                                }
                                return key;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        protected Position getNextPosition() throws IOException {
                            while (indexer.hasNext(indexFile)) {
                                int pos = indexFile.readInt();
                                byte fileNumber = indexFile.readByte();
                                if (fileNumber > 0) {
                                    return new Position(fileNumber, pos);
                                }
                            }
                            return null;
                        }

                    };
                }

                @Override
                public int size() {
                    try {
                        return indexer.getEntryCount();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear() {
        close();
        deleteDirectory();
        initialize();
    }

    @Override
    public void close() {
        dataManager.close();
        indexer.close();
    }

    protected void initialize() {
        this.dataManager = new DataManager<V>(this.dirPath);
        this.indexer = new Indexer(this.dirPath, this.bucketSize);
    }

    protected void deleteDirectory() {
        try {
            FileUtils.deleteDirectory(new File(this.dirPath));
        } catch (IOException ignore) {}

    }

    protected BSONObject readFrom(String key, RandomAccessFile indexFile, Position indexRef) throws IOException,
            FileNotFoundException {
        Position dataRef = indexer.getDataPosition(indexFile, indexRef);
        if (dataRef == null)
            return null;
        return dataManager.readFrom(key, dataRef.getPointer(), dataRef.getFileNumber());
    }

    protected BSONObject removeBSON(String key, int dataPos, byte dataFileNumber, RandomAccessFile indexFile,
                                    int indexPos, List<DataBlock> dataRefList) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("removeBSON, key:{}, dataPos:{}, dataFileNumber:{}, indexFile:{}, indexPos:{}, dataRefList:{}",
                         new Object[] { key, dataPos, dataFileNumber, indexFile, indexPos, dataRefList });
        }
        DataBlock dataBlock = dataManager.getDataBlock(dataPos, dataFileNumber);
        if (key.equals(dataManager.getKey(dataBlock))) {
            if ((dataBlock.getNextPointer() == 0) && (dataBlock.getNextFileNumber() == 0) && (dataRefList.size() == 0)) {
                // remain this one only
                indexer.clearIndex(indexFile, indexPos);
            } else if (dataRefList.size() > 0) {
                DataBlock lastDataRef = dataRefList.get(dataRefList.size() - 1);
                dataManager.updateNextRef(dataBlock, lastDataRef);
            } else {
                // remove at the first element.
                indexer.updateIndex(indexFile, indexPos, dataBlock.getNextPointer(), dataBlock.getNextFileNumber());
            }
            return dataBlock.getBsonObject();
        } else {
            dataRefList.add(dataBlock);
            return removeBSON(key, dataBlock.getNextPointer(), dataBlock.getNextFileNumber(), indexFile, indexPos,
                              dataRefList);
        }
    }

    public void updateIndex(RandomAccessFile indexFile, long dataPos, byte lastDataFileNumber) throws IOException {
        long indexPos = indexFile.getFilePointer();
        if (indexer.indexUpdatable(indexFile)) {
            indexer.updateIndex(indexFile, (int) indexPos, (int) dataPos, lastDataFileNumber);
        } else {
            indexFile.seek(indexPos);
            int dataRef = indexFile.readInt();
            byte dataFileNumber = indexFile.readByte();
            dataManager.updateNextRef((int) dataPos, lastDataFileNumber, dataRef, dataFileNumber);
        }
    }

    protected void dump(byte[] bin) {
        StringBuilder buf = new StringBuilder();
        for (byte b : bin) {
            buf.append(Integer.toHexString(b & 0xFF) + " ");
        }
        logger.debug(buf.toString());
    }

    @Override
    public int size() throws IOException {
        return indexer.getEntryCount();
    }
}
