package net.wrap_trap.collections.fsm;

public class Configuration {

    private static final String DEFAULT_DIR_PATH = "tmp";
    private static final int DEFAULT_BUCKET_SIZE = 4096;
    private static final int DEFAULT_DATA_FILE_SIZE = 1024 * 1024 * 512;

    private int bucketSize;
    private String dirPath;
    private long dataFileSize;

    public Configuration() {
        this.dirPath = DEFAULT_DIR_PATH;
        this.bucketSize = DEFAULT_BUCKET_SIZE;
        this.dataFileSize = DEFAULT_DATA_FILE_SIZE;
    }

    public long getDataFileSize() {
        return dataFileSize;
    }

    public void setDataFileSize(long dataFileSize) {
        this.dataFileSize = dataFileSize;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public String getDirPath() {
        return dirPath;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }
}
