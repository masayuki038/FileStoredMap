package net.wrap_trap.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public interface Store<V> extends Closeable {

    V get(String key) throws IOException;

    V put(String key, V v) throws IOException;

    V remove(String key) throws IOException;

    void clear() throws IOException;

    int size() throws IOException;

    Set<String> keySet() throws IOException;

    Set<java.util.Map.Entry<String, V>> entrySet() throws IOException;

    boolean containsKey(Object key) throws IOException;

    Collection<V> values() throws IOException;;

}
