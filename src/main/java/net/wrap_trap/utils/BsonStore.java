package net.wrap_trap.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BsonStore<V> implements Store<V> {

    protected static Logger logger = LoggerFactory.getLogger(BsonStore.class);

    private Indexer indexer;
    private DataManager<V> dataManager;

    @Override
    public void setDirectory(String path) {
        this.indexer = new Indexer(path);
        this.dataManager = new DataManager<V>(path);
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
            Position indexRef = indexer.getIndexRef(key);
            RandomAccessFile indexFile = indexer.getIndexFile(indexRef.getFileNumber());
            indexFile.seek(indexRef.getPointer());
            Position pos = dataManager.writeTo(indexFile, key, value);
            updateIndex(indexFile, pos.getPointer(), pos.getFileNumber());
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

        try {
            Position indexRef = indexer.getIndexRef(key);
            RandomAccessFile indexFile = indexer.getIndexFile(indexRef.getFileNumber());
            Position dataRef = indexer.getDataPosition(indexFile, indexRef);
            if (dataRef == null)
                return null;
            BSONObject bsonObject = removeBSON(key.toString(), dataRef.getPointer(), dataRef.getFileNumber(),
                                               indexFile, indexRef.getPointer(), new ArrayList<DataBlock>());
            return dataManager.rebuildValue(bsonObject);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        dataManager.close();
        indexer.close();
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

}
