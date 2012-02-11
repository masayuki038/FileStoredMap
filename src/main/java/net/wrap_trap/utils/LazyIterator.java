package net.wrap_trap.utils;

import java.util.Iterator;

public abstract class LazyIterator<E> implements Iterator<E> {

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
