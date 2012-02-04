package net.wrap_trap.utils;

public class IndexRef {

    private int fileNumber;
    private int pointer;

    public IndexRef(int fileNumber, int pointer) {
        super();
        this.fileNumber = fileNumber;
        this.pointer = pointer;
    }

    public int getFileNumber() {
        return fileNumber;
    }

    public int getPointer() {
        return pointer;
    }

}
