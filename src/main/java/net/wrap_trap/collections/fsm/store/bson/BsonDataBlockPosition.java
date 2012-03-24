package net.wrap_trap.collections.fsm.store.bson;

public class BsonDataBlockPosition {

    private byte fileNumber;
    private long pointer;

    public BsonDataBlockPosition(byte fileNumber, long pointer) {
        super();
        this.fileNumber = fileNumber;
        this.pointer = pointer;
    }

    public byte getFileNumber() {
        return fileNumber;
    }

    public long getPointer() {
        return pointer;
    }

    public boolean isEmpty() {
        return (pointer == 0L && fileNumber == 0);
    }

}
