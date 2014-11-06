package doss.local;

class BlobLocation {
    final private Long containerId;
    final private Long offset;

    public BlobLocation(Long containerId, Long offset) {
        this.containerId = containerId;
        this.offset = offset;
    }

    public Long containerId() {
        return containerId;
    }

    public Long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return containerId + ":" + offset;
    }
}