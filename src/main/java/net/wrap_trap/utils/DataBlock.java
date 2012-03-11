package net.wrap_trap.utils;

import org.bson.BSONObject;

public class DataBlock {

    private BSONObject bsonObject;
    private long currentPointer;
    private byte currentFileNumber;
    private long nextPointer;
    private byte nextFileNumber;

    public DataBlock(BSONObject bsonObject, long currentPointer, byte currentFileNumber, long nextPointer,
            byte nextFileNumber) {
        super();
        this.bsonObject = bsonObject;
        this.currentPointer = currentPointer;
        this.currentFileNumber = currentFileNumber;
        this.nextPointer = nextPointer;
        this.nextFileNumber = nextFileNumber;
    }

    public BSONObject getBsonObject() {
        return bsonObject;
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
}
