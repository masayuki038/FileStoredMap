package net.wrap_trap.utils;

import java.io.Closeable;
import java.io.IOException;

public interface Store<V> extends Closeable {

    V get(String key);

    V put(String key, V v);

    V remove(String key);

    void clear();

    int size() throws IOException;

}
