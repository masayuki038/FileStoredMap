package net.wrap_trap.utils;

import org.bson.BSONObject;

public class DataRef {

    private BSONObject bsonObject;
    private int currentPointer;
    private byte currentFileNumber;
    private int nextPointer;
    private byte nextFileNumber;

    public DataRef(BSONObject bsonObject, int currentPointer, byte currentFileNumber, int nextPointer,
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

    public int getCurrentPointer() {
        return currentPointer;
    }

    public byte getCurrentFileNumber() {
        return currentFileNumber;
    }

    public int getNextPointer() {
        return nextPointer;
    }

    public byte getNextFileNumber() {
        return nextFileNumber;
    }
}
