package doss;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

public interface BlobStore extends AutoCloseable {

    /**
     * Retrieve a Blob for reading.
     * 
     * @param blobId storage identifier for the target blob
     * @return the target blob
     * @throws IOException if an I/O error occurs
     * @throws NoSuchFileException if the blob does not exist 
     */
    Blob get(String blobId) throws NoSuchFileException, IOException;

    /**
     * Begins a new write transaction.
     * 
     * @return the new transaction
     */
    BlobTx begin();

}
