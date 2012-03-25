package net.wrap_trap.collections.fsm.store.bson;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import net.wrap_trap.collections.fsm.Configuration;
import net.wrap_trap.monganez.BSONObjectMapper;

import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class BsonEntityService<V> implements Closeable {

    protected static Logger logger = LoggerFactory.getLogger(BsonEntityService.class);

    private BSONEncoder encoder = new BSONEncoder();
    private BSONObjectMapper objectMapper = new BSONObjectMapper();
    private EntityRepository repository;

    public BsonEntityService(Configuration configuration) {
        this.repository = new RandomAccessFileEntityRepository(configuration);
    }

    public BSONObject readFrom(String key, BsonDataBlockPosition dataRef) throws IOException, FileNotFoundException {
        if (logger.isTraceEnabled()) {
            logger.trace("readFrom, key:{}, dataRef:{}", new Object[] { key, dataRef });
        }
        if (dataRef.isEmpty())
            // index record is empty.(this record area has cleaned up.)
            return null;
        return readDataFile(key, dataRef);
    }

    public void updateDataBlockLink(BsonDataBlock from, BsonDataBlock to) throws IOException {
        repository.updateDataBlockLink(from, to);
    }

    public void updateDataBlockLink(BsonDataBlockPosition from, BsonDataBlockPosition to) throws FileNotFoundException,
            IOException {
        repository.updateDataBlockLink(from, to);
    }

    public BsonDataBlockPosition getLastDataBlockPosition(BsonDataBlockPosition start) throws IOException {
        return repository.getLastDataBlockPosition(start);
    }

    @Override
    public void close() throws IOException {
        repository.close();
    }

    protected BSONObject readDataFile(String key, BsonDataBlockPosition dataRef) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("readDataFile, key:{}, dataRef:{}", new Object[] { key, dataRef });
        }
        BsonDataBlock bsonDataBlock = getDataBlock(dataRef);
        if (key.equals(getKey(bsonDataBlock)))
            return bsonDataBlock.getBsonObject();
        return readFrom(key,
                        new BsonDataBlockPosition(bsonDataBlock.getNextFileNumber(), bsonDataBlock.getNextPointer()));
    }

    public BsonDataBlock getDataBlock(BsonDataBlockPosition dataRef) throws IOException {
        return repository.getDataBlock(dataRef);
    }

    public String getKey(BsonDataBlock bsonDataBlock) {
        Set<String> bsonKeys = bsonDataBlock.getBsonObject().keySet();
        Preconditions.checkArgument(bsonKeys.size() == 1);
        for (String bsonKey : bsonKeys)
            return bsonKey;
        throw new IllegalArgumentException("BsonObject is not key-value forms.");
    }

    public BsonDataBlockPosition writeTo(String key, V value) throws IOException {
        try {
            byte[] bytes = toByteArray(key, value);
            return repository.writeTo(bytes);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected byte[] toByteArray(String key, V v) throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        BSONObject object = objectMapper.createBSONObject(key, v);
        return encoder.encode(object);
    }

    @SuppressWarnings("unchecked")
    protected V rebuildValue(BSONObject object) {
        try {
            Set<String> keySet = object.keySet();
            Preconditions.checkArgument(keySet.size() == 1);

            for (String key : keySet) {
                Object target = object.get(key);
                Object v = null;
                if (target instanceof BSONObject) {
                    v = objectMapper.toObject((BSONObject) target);
                } else {
                    v = target;
                }
                return (V) v;
            }
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        }
        return null;
    }
}
