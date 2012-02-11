package net.wrap_trap.utils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStoredMap<V> implements Map<String, V> {

    protected static Logger logger = LoggerFactory.getLogger(FileStoredMap.class);

    private Store<V> store;
    private String dirPath;

    public FileStoredMap(String dirPath) {
        this(dirPath, 4096);
    }

    public FileStoredMap(String dirPath, int bucketSize) {
        this.dirPath = dirPath;
        initializeDirectory();
        store = new BsonStore<V>(dirPath, bucketSize);
    }

    public void clear() {
        if (logger.isTraceEnabled()) {
            logger.trace("clear, ");
        }
        store.clear();
        initializeDirectory();
    }

    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException();
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public Set<java.util.Map.Entry<String, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    public V get(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("get, key:{}", key);
        }
        return store.get(key.toString());
    }

    public boolean isEmpty() {
        if (logger.isTraceEnabled()) {
            logger.trace("isEmpty, ");
        }
        try {
            return (store.size() == 0);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Set<String> keySet() {
        if (logger.isTraceEnabled()) {
            logger.trace("isEmpty, ");
        }
        return store.keySet();
    }

    public V remove(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        return store.remove(key.toString());
    }

    public V put(String key, V value) {
        if (logger.isTraceEnabled()) {
            logger.trace("put, key:{}, value:{}", key, value);
        }
        return store.put(key, value);
    }

    public void putAll(Map<? extends String, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        if (logger.isTraceEnabled()) {
            logger.trace("size, ");
        }
        try {
            return store.size();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
        store.close();
    }

    protected void initializeDirectory() {
        new File(this.dirPath).mkdir();
    }
}
