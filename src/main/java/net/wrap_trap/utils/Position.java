package net.wrap_trap.utils;

public class Position {

    private byte fileNumber;
    private long pointer;

    public Position(byte fileNumber, long pointer) {
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

}
