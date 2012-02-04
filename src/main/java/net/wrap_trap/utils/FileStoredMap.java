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

    private Store<V> store = new BsonStore<V>();

    public FileStoredMap(String dirPath) {
        File dir = new File(dirPath);
        dir.mkdir();
        store.setDirectory(dirPath);
    }

    public void clear() {
        throw new NotImplementedException();
    }

    public boolean containsKey(Object key) {
        throw new NotImplementedException();
    }

    public boolean containsValue(Object value) {
        throw new NotImplementedException();
    }

    public Set<java.util.Map.Entry<String, V>> entrySet() {
        throw new NotImplementedException();
    }

    public V get(Object key) {
        if (logger.isTraceEnabled()) {
            logger.trace("get, key:{}", key);
        }
        return store.get(key.toString());
    }

    public boolean isEmpty() {
        throw new NotImplementedException();
    }

    public Set<String> keySet() {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
    }

    public int size() {
        throw new NotImplementedException();
    }

    public Collection<V> values() {
        throw new NotImplementedException();
    }

    public void close() throws IOException {
        store.close();
    }
}
