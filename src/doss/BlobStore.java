package doss;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

public interface BlobStore extends AutoCloseable {

    /**
     * Retrieve a Blob for reading.
     * 
     * @param blobId
     *            storage identifier for the target blob
     * @return the target blob
     * @throws IOException
     *             if an I/O error occurs
     * @throws NoSuchFileException
     *             if the blob does not exist
     */
    Blob get(long blobId) throws NoSuchBlobException, IOException;

    /**
     * Begins a new write transaction.
     * 
     * @return the new transaction
     */
    BlobTx begin();

    /**
     * Resumes an uncommitted write transaction.
     * 
     * @param txId
     *            an id previously obtained from {@link BlobTx#id()}
     * @return the resumed transaction
     */
    BlobTx resume(long txId) throws NoSuchBlobTxException;

    @Override
    void close();
}
