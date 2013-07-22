package doss.core;

import doss.NoSuchBlobException;

/**
 * Interface for reading a {@link Container} index.
 * 
 * @see ContainerIndexWriter
 */

public interface ContainerIndexReader {
    /**
     * Locates a {@link Blob} in the {@link Container}
     * 
     * @param blobId
     * @return offset into the {@link Container}
     * @throws NoSuchBlobException
     *             if the blob does not exist
     */
    long locate(long blobId) throws NoSuchBlobException;
}
