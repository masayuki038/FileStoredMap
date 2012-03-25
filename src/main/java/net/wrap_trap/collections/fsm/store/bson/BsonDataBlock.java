package net.wrap_trap.collections.fsm.store.bson;

import org.bson.BSONDecoder;
import org.bson.BSONObject;

public class BsonDataBlock {

    private BSONDecoder decoder = new BSONDecoder();

    private byte[] body;
    private long currentPointer;
    private byte currentFileNumber;
    private long nextPointer;
    private byte nextFileNumber;
    private BSONObject bsonObject;

    public BsonDataBlock(byte[] body, long currentPointer, byte currentFileNumber, long nextPointer, byte nextFileNumber) {
        super();
        this.body = body;
        this.currentPointer = currentPointer;
        this.currentFileNumber = currentFileNumber;
        this.nextPointer = nextPointer;
        this.nextFileNumber = nextFileNumber;
    }

    public byte[] getBoby() {
        return body;
    }

    public long getCurrentPointer() {
        return currentPointer;
    }

    public byte getCurrentFileNumber() {
        return currentFileNumber;
    }

    public long getNextPointer() {
        return nextPointer;
    }

    public byte getNextFileNumber() {
        return nextFileNumber;
    }

    public BSONObject getBsonObject() {
        if (bsonObject == null) {
            bsonObject = decoder.readObject(body);
        }
        return bsonObject;
    }
}
