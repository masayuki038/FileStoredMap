package net.wrap_trap.utils;

public class Position {

    private byte fileNumber;
    private int pointer;

    public Position(byte fileNumber, int pointer) {
        super();
        this.fileNumber = fileNumber;
        this.pointer = pointer;
    }

    public byte getFileNumber() {
        return fileNumber;
    }

    public int getPointer() {
        return pointer;
    }

}
