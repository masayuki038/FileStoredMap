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
        try {
            store.clear();
            initializeDirectory();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException();
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public Set<java.util.Map.Entry<String, V>> entrySet() {
        if (logger.isTraceEnabled()) {
            logger.trace("entrySet, ");
        }
        try {
            return store.entrySet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public V get(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("get, key:{}", key);
        }
        try {
            return store.get(key.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        try {
            return store.keySet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public V remove(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("remove, key:{}", key);
        }
        try {
            return store.remove(key.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public V put(String key, V value) {
        if (logger.isTraceEnabled()) {
            logger.trace("put, key:{}, value:{}", key, value);
        }
        try {
            return store.put(key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
