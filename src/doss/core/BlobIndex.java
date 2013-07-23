package doss.core;

import doss.NoSuchBlobException;

/**
 * The BlobIndex allows blobs to be quickly located within a BlobStore. For each
 * blob it stores the id of the container the blob is located within and the
 * position (offset) of the blob within that container.
 */
public interface BlobIndex {

    /**
     * Locates the container a blob is stored in and the position (byte offset)
     * of the blob within that container.
     * 
     * @param blobId
     * @return the container and offset of the blob within that container
     * @throws NoSuchBlobException
     *             if the blob does not exist
     */
    BlobIndexEntry locate(long blobId) throws NoSuchBlobException;

    /**
     * Remembers the location of a blob.
     * 
     * @param blobId
     *            the blob to remember
     * @param container
     *            the container which the blob was stored in
     * @param offset
     *            the position of the blob within the container
     */
    void remember(long blobId, Container container, long offset);

    /**
     * Marks this blob as deleted.
     * 
     * @param blobId
     *            the blob to delete
     */
    void delete(long blobId);

}
