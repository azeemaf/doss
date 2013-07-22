package doss.core;

/**
 * Interface for writing to an index of a {@link Container}.
 * 
 * @see ContainerIndexReader
 */
public interface ContainerIndexWriter {
    /**
     * Remembers the location of a {@link Blob}.
     * 
     * @param blobId
     *            the ID of the {@link Blob} to remember
     * @param offset
     *            the offset within the {@link Container} to remember
     */
    void remember(long blobId, long offset);

    /**
     * Marks this {@link Blob} as deleted.
     * 
     * @param blobId
     *            the ID of the {@link Blob} to delete
     */
    void delete(long blobId);
}
