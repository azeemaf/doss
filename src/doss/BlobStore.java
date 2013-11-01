package doss;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

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
     * Retrieve a Blob by DOSSv1 path.
     * 
     * @param blobId
     *            storage identifier for the target blob
     * @return the target blob
     * @throws IOException
     *             if an I/O error occurs
     * @throws NoSuchFileException
     *             if the blob does not exist
     */
    Blob getLegacy(Path legacyPath) throws NoSuchBlobException, IOException;

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
