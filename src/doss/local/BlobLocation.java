package doss.local;

class BlobLocation {
    final private long blobId;
    final private Long containerId;
    final private Long offset;
    final private int containerState;

    public BlobLocation(long blobId, Long containerId, Long offset, int containerState) {
        this.blobId = blobId;
        this.containerId = containerId;
        this.offset = offset;
        this.containerState = containerState;
    }

    public long blobId() {
        return blobId;
    }

    public Long containerId() {
        return containerId;
    }

    public Long offset() {
        return offset;
    }

    public int containerState() {
        return containerState;
    }

    public boolean isInStagingArea() {
        return containerId == null || offset == null ||
                (containerState != Database.CNT_WRITTEN &&
                containerState != Database.CNT_ARCHIVED);
    }

    @Override
    public String toString() {
        return containerId + ":" + offset;
    }

}