package doss;

import java.io.FileNotFoundException;

public interface BlobStore extends AutoCloseable {

    /**
     * Retrieve a Blob for reading.
     * 
     * @param blobId storage identifier for the target blob
     * @return the target blob
     * @throws FileNotFoundException if the blob does not exist 
     */
    Blob get(String blobId) throws FileNotFoundException;

    /**
     * Begins a new write transaction.
     * 
     * @return the new transaction
     */
    BlobTx begin();

}
