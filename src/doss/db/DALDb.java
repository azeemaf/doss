package doss.db;

public interface DALDb {

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
     * @param blobId
     * @param offset
     */
    void remember(long blobId, long offset);

    /**
     * Marks this blob as deleted.
     * 
     * @param blobId
     */
    void delete(long blobId);

}
