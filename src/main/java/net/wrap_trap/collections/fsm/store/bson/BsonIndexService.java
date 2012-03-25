package net.wrap_trap.collections.fsm.store.bson;

import java.io.Closeable;
import java.io.IOException;

import net.wrap_trap.collections.fsm.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 *  structure of index
 * +--+--+--+--+--+--+--+--+--+
 * |           a.          |b.|
 * +--+-+--+--+--+---+--+--+--+
 * 
 * a. a offset of data file.[long]
 * b. data file number(1-2).[byte]
 * </pre>
 */
public class BsonIndexService implements Closeable {

    protected static Logger logger = LoggerFactory.getLogger(BsonIndexService.class);

    private RandomAccessFileIndexRepository repository;

    public BsonIndexService(Configuration configuration) throws IOException {
        super();
        this.repository = new RandomAccessFileIndexRepository(configuration);
    }

    public BsonDataBlockPosition getIndexRef(String key) {
        long hashCode = toUnsignedInt(key.hashCode());
        return repository.getIndexRef(hashCode);
    }

    public BsonDataBlockPosition getDataPosition(String key) throws IOException {
        return getDataPosition(getIndexRef(key));
    }

    public BsonDataBlockPosition getDataPosition(BsonDataBlockPosition indexRef) throws IOException {
        return repository.getDataPosition(indexRef);
    }

    public void updateIndex(BsonDataBlockPosition indexRef, BsonDataBlockPosition dataRef) throws IOException {
        repository.updateIndex(indexRef, dataRef);
    }

    public boolean indexUpdatable(BsonDataBlockPosition indexRef) throws IOException {
        return repository.indexUpdatable(indexRef);
    }

    @Override
    public void close() {
        repository.close();
    }

    public void clearIndex(BsonDataBlockPosition indexRef) throws IOException {
        repository.clearIndex(indexRef);
    }

    protected long toUnsignedInt(int i) {
        return i & 0xffffffffL;
    }

    public void incrementEntryCount() throws IOException {
        int count = repository.getEntryCount();
        repository.setEntryCount(count + 1);
    }

    public void decrementEntryCount() throws IOException {
        int count = repository.getEntryCount();
        repository.setEntryCount(count - 1);
    }

    public int getEntryCount() throws IOException {
        return repository.getEntryCount();
    }

    public void resetPosition() throws IOException {
        repository.seekIndexHead();
    }

    public boolean hasNext() throws IOException {
        return repository.hasNext();
    }

    public BsonDataBlockPosition read() throws IOException {
        return repository.read();
    }
}
