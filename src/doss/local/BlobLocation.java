package doss.local;

class BlobLocation {
    final private String area;
    final private long containerId;
    final private long offset;

    public BlobLocation(String area, long containerId, long offset) {
        this.containerId = containerId;
        this.offset = offset;
        this.area = area;
    }

    public long containerId() {
        return containerId;
    }

    public long offset() {
        return offset;
    }

    public String area() {
        return area;
    }

    @Override
    public String toString() {
        return area + ":" + containerId + ":" + offset;
    }
}