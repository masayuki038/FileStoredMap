package net.wrap_trap.collections.fsm.store.bson.utils;

import java.util.Iterator;

public abstract class LazyIterator<E> implements Iterator<E> {

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
