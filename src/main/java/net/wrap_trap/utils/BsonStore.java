package net.wrap_trap.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
    public V get(String key) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("get, key:{}", key);
        }
        try {
            BSONObject bsonObject = readFrom(key);
            if (bsonObject == null)
                return null;
            return dataManager.rebuildValue(bsonObject);
        } catch (FileNotFoundException ex) {
            return null;
        }
    }

    @Override
    public V put(String key, V value) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("put, key:{}, value:{}", key, value);
        }
        V pre = get(key);
        if (pre != null) {
            remove(key);
        }
        synchronized (this) {
            Position indexRef = indexer.getIndexRef(key);
            Position dataRef = dataManager.writeTo(key, value);
            updateIndex(indexRef, dataRef);
            indexer.incrementEntryCount();
        }
        return pre;
    }

    @Override
    public V remove(String key) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        synchronized (this) {
            Position indexRef = indexer.getIndexRef(key);
            Position dataRef = indexer.getDataPosition(indexRef);
            if (dataRef == null)
                return null;
            BSONObject bsonObject = removeBSON(key, indexRef, dataRef, new ArrayList<DataBlock>());
            indexer.decrementEntryCount();
            return dataManager.rebuildValue(bsonObject);
        }
    }

    @Override
    public Set<String> keySet() throws IOException {
        return createKeyIterator();
    }

    protected Set<String> createKeyIterator() {
        return new LazySet<String>() {
            @Override
            public Iterator<String> iterator() {
                return new LazyIterator<String>() {

                    Position dataRef;
                    DataBlock dataBlock;

                    {
                        try {
                            indexer.seekIndexHead();
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
                                dataBlock = dataManager.getDataBlock(dataRef);
                            }
                            String key = dataManager.getKey(dataBlock);
                            if (dataBlock.getNextFileNumber() == 0) {
                                dataBlock = null;
                                dataRef = getNextPosition();
                            } else {
                                dataBlock = dataManager.getDataBlock(new Position(dataBlock.getNextFileNumber(),
                                                                                  dataBlock.getNextPointer()));
                            }
                            return key;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    protected Position getNextPosition() throws IOException {
                        while (indexer.hasNext()) {
                            Position indexRef = indexer.read();
                            if (indexRef != null) {
                                return indexRef;
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
    }

    @Override
    public Set<java.util.Map.Entry<String, V>> entrySet() throws IOException {
        return createEntrySetIterator();
    }

    private Set<Entry<String, V>> createEntrySetIterator() throws IOException {
        final Iterator<String> keyIterator = keySet().iterator();

        return new LazySet<Entry<String, V>>() {
            @Override
            public Iterator<Entry<String, V>> iterator() {
                return new LazyIterator<Entry<String, V>>() {

                    @Override
                    public boolean hasNext() {
                        return keyIterator.hasNext();
                    }

                    @Override
                    public Entry<String, V> next() {
                        Preconditions.checkState(hasNext());
                        try {
                            final String key = keyIterator.next();
                            final V v = get(key);
                            Preconditions.checkNotNull(v);
                            return new Map.Entry<String, V>() {

                                @Override
                                public String getKey() {
                                    return key;
                                }

                                @Override
                                public V getValue() {
                                    return v;
                                }

                                @Override
                                public V setValue(V value) {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
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

    protected BSONObject readFrom(String key) throws IOException, FileNotFoundException {
        Position dataRef = indexer.getDataPosition(key);
        if (dataRef == null)
            return null;
        return dataManager.readFrom(key, dataRef);
    }

    protected BSONObject removeBSON(String key, Position indexRef, Position dataRef, List<DataBlock> dataRefList)
            throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("removeBSON, key:{}, dataRef{}, indexRef:{}, dataRefList:{}", new Object[] { key, dataRef,
                                                                                                     indexRef,
                                                                                                     dataRefList });
        }
        DataBlock dataBlock = dataManager.getDataBlock(dataRef);
        if (key.equals(dataManager.getKey(dataBlock))) {
            if ((dataBlock.getNextPointer() == 0) && (dataBlock.getNextFileNumber() == 0) && (dataRefList.size() == 0)) {
                // remain this one only
                indexer.clearIndex(indexRef);
            } else if (dataRefList.size() > 0) {
                DataBlock lastDataRef = dataRefList.get(dataRefList.size() - 1);
                dataManager.updateNextRef(dataBlock, lastDataRef);
            } else {
                // remove at the first element.
                indexer.updateIndex(indexRef, dataRef);
            }
            return dataBlock.getBsonObject();
        } else {
            dataRefList.add(dataBlock);
            return removeBSON(key, indexRef, new Position(dataBlock.getNextFileNumber(), dataBlock.getNextPointer()),
                              dataRefList);
        }
    }

    public void updateIndex(Position indexRef, Position nextDataRef) throws IOException {
        if (indexer.indexUpdatable(indexRef)) {
            indexer.updateIndex(indexRef, nextDataRef);
        } else {
            Position rootDataRef = indexer.getDataPosition(indexRef);
            dataManager.updateNextRef(rootDataRef, nextDataRef);
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

    @Override
    public boolean containsKey(Object key) throws FileNotFoundException, IOException {
        BSONObject bsonObject = readFrom(key.toString());
        return (bsonObject != null);
    }
}
