package net.wrap_trap.collections.fsm.store.bson;

import java.io.Closeable;
import java.io.IOException;

public interface IndexRepository extends Closeable {

    BsonDataBlockPosition getIndexRef(long hashCode);

    BsonDataBlockPosition getDataPosition(BsonDataBlockPosition indexRef) throws IOException;

    void updateIndex(BsonDataBlockPosition indexRef, BsonDataBlockPosition dataRef) throws IOException;

    boolean indexUpdatable(BsonDataBlockPosition indexRef) throws IOException;

    void clearIndex(BsonDataBlockPosition indexRef) throws IOException;

    void setEntryCount(int count) throws IOException;

    int getEntryCount() throws IOException;

    void seekIndexHead() throws IOException;

    boolean hasNext() throws IOException;

    BsonDataBlockPosition read() throws IOException;

}