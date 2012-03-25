package net.wrap_trap.collections.fsm.store.bson;

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

import net.wrap_trap.collections.fsm.Configuration;
import net.wrap_trap.collections.fsm.store.Store;
import net.wrap_trap.collections.fsm.store.bson.utils.LazyIterator;
import net.wrap_trap.collections.fsm.store.bson.utils.LazySet;

import org.apache.commons.io.FileUtils;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class BsonStore<V> implements Store<V> {

    protected static Logger logger = LoggerFactory.getLogger(BsonStore.class);

    private BsonIndexService bsonIndexService;
    private BsonEntityService<V> bsonEntityService;
    private Configuration configuration;

    public BsonStore(Configuration configuration) throws IOException {
        this.configuration = configuration;
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
            return bsonEntityService.rebuildValue(bsonObject);
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
            BsonDataBlockPosition indexRef = bsonIndexService.getIndexRef(key);
            BsonDataBlockPosition dataRef = bsonEntityService.writeTo(key, value);
            updateIndex(indexRef, dataRef);
            bsonIndexService.incrementEntryCount();
        }
        return pre;
    }

    @Override
    public V remove(String key) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        synchronized (this) {
            BsonDataBlockPosition indexRef = bsonIndexService.getIndexRef(key);
            BsonDataBlockPosition dataRef = bsonIndexService.getDataPosition(indexRef);
            if (dataRef == null)
                return null;
            BSONObject bsonObject = removeBSON(key, indexRef, dataRef, new ArrayList<BsonDataBlock>());
            bsonIndexService.decrementEntryCount();
            return bsonEntityService.rebuildValue(bsonObject);
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

                    BsonDataBlockPosition dataRef;
                    BsonDataBlock bsonDataBlock;

                    {
                        try {
                            bsonIndexService.resetPosition();
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
                            if (this.bsonDataBlock == null) {
                                bsonDataBlock = bsonEntityService.getDataBlock(dataRef);
                            }
                            String key = bsonEntityService.getKey(bsonDataBlock);
                            if (bsonDataBlock.getNextFileNumber() == 0) {
                                bsonDataBlock = null;
                                dataRef = getNextPosition();
                            } else {
                                bsonDataBlock = bsonEntityService.getDataBlock(new BsonDataBlockPosition(
                                                                                                         bsonDataBlock.getNextFileNumber(),
                                                                                                         bsonDataBlock.getNextPointer()));
                            }
                            return key;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    protected BsonDataBlockPosition getNextPosition() throws IOException {
                        while (bsonIndexService.hasNext()) {
                            BsonDataBlockPosition indexRef = bsonIndexService.read();
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
                    return bsonIndexService.getEntryCount();
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
                    return bsonIndexService.getEntryCount();
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
                    return bsonIndexService.getEntryCount();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    @Override
    public void clear() throws IOException {
        close();
        deleteDirectory();
    }

    @Override
    public void close() throws IOException {
        bsonEntityService.close();
        bsonIndexService.close();
    }

    public void initialize() throws IOException {
        new File(this.configuration.getDirPath()).mkdir();
        this.bsonEntityService = new BsonEntityService<V>(this.configuration);
        this.bsonIndexService = new BsonIndexService(this.configuration);
    }

    protected void deleteDirectory() {
        try {
            FileUtils.deleteDirectory(new File(configuration.getDirPath()));
        } catch (IOException ignore) {}

    }

    protected BSONObject readFrom(String key) throws IOException, FileNotFoundException {
        BsonDataBlockPosition dataRef = bsonIndexService.getDataPosition(key);
        if (dataRef == null)
            return null;
        return bsonEntityService.readFrom(key, dataRef);
    }

    protected BSONObject removeBSON(String key, BsonDataBlockPosition indexRef, BsonDataBlockPosition dataRef,
                                    List<BsonDataBlock> dataRefList) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("removeBSON, key:{}, dataRef{}, indexRef:{}, dataRefList:{}", new Object[] { key, dataRef,
                                                                                                     indexRef,
                                                                                                     dataRefList });
        }
        BsonDataBlock bsonDataBlock = bsonEntityService.getDataBlock(dataRef);
        if (key.equals(bsonEntityService.getKey(bsonDataBlock))) {
            if ((bsonDataBlock.getNextPointer() == 0) && (bsonDataBlock.getNextFileNumber() == 0) &&
                (dataRefList.size() == 0)) {
                // remain this one only
                bsonIndexService.clearIndex(indexRef);
            } else if (dataRefList.size() > 0) {
                BsonDataBlock lastDataRef = dataRefList.get(dataRefList.size() - 1);
                bsonEntityService.updateDataBlockLink(lastDataRef, bsonDataBlock);
            } else {
                // remove at the first element.
                bsonIndexService.updateIndex(indexRef, dataRef);
            }
            return bsonDataBlock.getBsonObject();
        } else {
            dataRefList.add(bsonDataBlock);
            return removeBSON(key,
                              indexRef,
                              new BsonDataBlockPosition(bsonDataBlock.getNextFileNumber(),
                                                        bsonDataBlock.getNextPointer()), dataRefList);
        }
    }

    public void updateIndex(BsonDataBlockPosition indexRef, BsonDataBlockPosition newData) throws IOException {
        if (bsonIndexService.indexUpdatable(indexRef)) {
            bsonIndexService.updateIndex(indexRef, newData);
        } else {
            BsonDataBlockPosition root = bsonIndexService.getDataPosition(indexRef);
            BsonDataBlockPosition last = bsonEntityService.getLastDataBlockPosition(root);
            bsonEntityService.updateDataBlockLink(last, newData);
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
        return bsonIndexService.getEntryCount();
    }

    @Override
    public boolean containsKey(Object key) throws FileNotFoundException, IOException {
        BSONObject bsonObject = readFrom(key.toString());
        return (bsonObject != null);
    }
}
