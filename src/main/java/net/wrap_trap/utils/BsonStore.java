package net.wrap_trap.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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

    private IndexService indexService;
    private EntityService<V> entityService;
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
            return entityService.rebuildValue(bsonObject);
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
            Position indexRef = indexService.getIndexRef(key);
            Position dataRef = entityService.writeTo(key, value);
            updateIndex(indexRef, dataRef);
            indexService.incrementEntryCount();
        }
        return pre;
    }

    @Override
    public V remove(String key) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        synchronized (this) {
            Position indexRef = indexService.getIndexRef(key);
            Position dataRef = indexService.getDataPosition(indexRef);
            if (dataRef == null)
                return null;
            BSONObject bsonObject = removeBSON(key, indexRef, dataRef, new ArrayList<DataBlock>());
            indexService.decrementEntryCount();
            return entityService.rebuildValue(bsonObject);
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
                            indexService.seekIndexHead();
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
                                dataBlock = entityService.getDataBlock(dataRef);
                            }
                            String key = entityService.getKey(dataBlock);
                            if (dataBlock.getNextFileNumber() == 0) {
                                dataBlock = null;
                                dataRef = getNextPosition();
                            } else {
                                dataBlock = entityService.getDataBlock(new Position(dataBlock.getNextFileNumber(),
                                                                                  dataBlock.getNextPointer()));
                            }
                            return key;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    protected Position getNextPosition() throws IOException {
                        while (indexService.hasNext()) {
                            Position indexRef = indexService.read();
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
                    return indexService.getEntryCount();
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
                    return indexService.getEntryCount();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    @Override
    public Collection<V> values() throws IOException {
        return createValuesIterator();
    }

    protected Collection<V> createValuesIterator() throws IOException {
        final Iterator<String> keyIterator = keySet().iterator();

        return new LazySet<V>() {
            @Override
            public Iterator<V> iterator() {
                return new LazyIterator<V>() {

                    @Override
                    public boolean hasNext() {
                        return keyIterator.hasNext();
                    }

                    @Override
                    public V next() {
                        Preconditions.checkState(hasNext());
                        try {
                            final String key = keyIterator.next();
                            final V v = get(key);
                            Preconditions.checkNotNull(v);
                            return v;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            }

            @Override
            public int size() {
                try {
                    return indexService.getEntryCount();
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
        entityService.close();
        indexService.close();
    }

    protected void initialize() {
        this.entityService = new EntityService<V>(this.dirPath);
        this.indexService = new IndexService(this.dirPath, this.bucketSize);
    }

    protected void deleteDirectory() {
        try {
            FileUtils.deleteDirectory(new File(this.dirPath));
        } catch (IOException ignore) {}

    }

    protected BSONObject readFrom(String key) throws IOException, FileNotFoundException {
        Position dataRef = indexService.getDataPosition(key);
        if (dataRef == null)
            return null;
        return entityService.readFrom(key, dataRef);
    }

    protected BSONObject removeBSON(String key, Position indexRef, Position dataRef, List<DataBlock> dataRefList)
            throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("removeBSON, key:{}, dataRef{}, indexRef:{}, dataRefList:{}", new Object[] { key, dataRef,
                                                                                                     indexRef,
                                                                                                     dataRefList });
        }
        DataBlock dataBlock = entityService.getDataBlock(dataRef);
        if (key.equals(entityService.getKey(dataBlock))) {
            if ((dataBlock.getNextPointer() == 0) && (dataBlock.getNextFileNumber() == 0) && (dataRefList.size() == 0)) {
                // remain this one only
                indexService.clearIndex(indexRef);
            } else if (dataRefList.size() > 0) {
                DataBlock lastDataRef = dataRefList.get(dataRefList.size() - 1);
                entityService.updateNextRef(dataBlock, lastDataRef);
            } else {
                // remove at the first element.
                indexService.updateIndex(indexRef, dataRef);
            }
            return dataBlock.getBsonObject();
        } else {
            dataRefList.add(dataBlock);
            return removeBSON(key, indexRef, new Position(dataBlock.getNextFileNumber(), dataBlock.getNextPointer()),
                              dataRefList);
        }
    }

    public void updateIndex(Position indexRef, Position nextDataRef) throws IOException {
        if (indexService.indexUpdatable(indexRef)) {
            indexService.updateIndex(indexRef, nextDataRef);
        } else {
            Position rootDataRef = indexService.getDataPosition(indexRef);
            entityService.updateNextRef(rootDataRef, nextDataRef);
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
        return indexService.getEntryCount();
    }

    @Override
    public boolean containsKey(Object key) throws FileNotFoundException, IOException {
        BSONObject bsonObject = readFrom(key.toString());
        return (bsonObject != null);
    }
}
