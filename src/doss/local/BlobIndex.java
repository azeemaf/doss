package doss.local;

import doss.NoSuchBlobException;


/**
 * A index that maps blob ids to offsets within containers.
 * 
 * TODO: currently assumes there is a single universal container :)
 */
public interface BlobIndex {

    /**
     * Locates a blob in the container
     * 
     * @param blobId
     * @return offset into the container file
     * @throws NoSuchBlobException if the blob does not exist
     */
    long locate(long blobId) throws NoSuchBlobException;

    /**
     * Remembers the location of a blob.
     * 
     * @param blobId the blob to remember
     * @param offset the offset within the container to remember
     */
    void remember(long blobId, long offset);

    /**
     * Marks this blob as deleted.
     * 
     * @param blobId the blob to delete
     */
    void delete(long blobId);

}
