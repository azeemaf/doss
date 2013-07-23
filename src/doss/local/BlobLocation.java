package doss.local;

class BlobLocation {
    final private long containerId;
    final private long offset;

    public BlobLocation(long containerId, long offset) {
        this.containerId = containerId;
        this.offset = offset;
    }

    public long containerId() {
        return containerId;
    }

    public long offset() {
        return offset;
    }
}