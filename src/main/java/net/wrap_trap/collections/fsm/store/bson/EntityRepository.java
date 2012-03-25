package net.wrap_trap.collections.fsm.store.bson;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;

public interface EntityRepository extends Closeable {

    BsonDataBlock getDataBlock(BsonDataBlockPosition dataRef) throws IOException;

    BsonDataBlockPosition getLastDataBlockPosition(BsonDataBlockPosition start) throws IOException;

    BsonDataBlockPosition writeTo(byte[] bytes) throws IOException;

    void updateDataBlockLink(BsonDataBlock from, BsonDataBlock to) throws IOException;

    void updateDataBlockLink(BsonDataBlockPosition from, BsonDataBlockPosition to) throws FileNotFoundException,
            IOException;
}