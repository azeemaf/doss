package doss.core;

/**
 * An index entry consisting of a container id and offset within that
 * container.
 * 
 * @see BlobIndex
 */
public class BlobIndexEntry {
    final long containerId;
    final long offset;

    public BlobIndexEntry(long containerId, long offset) {
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