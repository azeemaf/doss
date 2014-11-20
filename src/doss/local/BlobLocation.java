package doss.local;

class BlobLocation {
    final private long blobId;
    final private Long containerId;
    final private Long offset;
    final private Integer containerState;
    final private Long txId;

    public BlobLocation(long blobId, Long containerId, Long offset, Integer containerState,
            Long txId) {
        this.blobId = blobId;
        this.containerId = containerId;
        this.offset = offset;
        this.containerState = containerState;
        this.txId = txId;
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

    public Long txId() {
        return txId;
    }

    public Integer containerState() {
        return containerState;
    }

    public boolean isInStagingArea() {
        return containerId == null || offset == null ||
                containerState == null ||
                (!containerState.equals(Database.CNT_WRITTEN) &&
                !containerState.equals(Database.CNT_ARCHIVED));
    }

    @Override
    public String toString() {
        return containerId + ":" + offset;
    }

}