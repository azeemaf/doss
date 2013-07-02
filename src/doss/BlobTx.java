package doss;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A transaction for writing to a BlobStore.
 */
public interface BlobTx extends Named, AutoCloseable {

    /**
     * Store a new blob.
     * 
     * @param channelOutput something that can be written to a byte channel
     * @return the new blob
     * @throws IOException if an I/O error occurs
     */
    Blob put(Writable output) throws IOException;
    
    /**
     * Store a local file as a new blob.
     * 
     * @param source path to the file to store
     * @return the new blob
     * @throws IOException if an I/O error occurs 
     */
    Blob put(Path source) throws IOException;
    
    /**
     * Store the contents of a byte array as a new blob.
     * 
     * @param bytes bytes to store 
     * @return the new blob
     * @throws IOException if an I/O error occurs
     */
    Blob put(byte[] bytes) throws IOException;
    
    /**
     * Store the contents of a string as a new blob. The String will be encoded with UTF-8.
     * 
     * @param source contents to store
     * @return the new blob
     * @throws IOException if an I/O error occurs 
     */
    Blob put(String contents) throws IOException;
    
    /**
     * Commits the transaction, ensuring all blobs written in this transaction
     * are persisted in permanent storage.
     * 
     * @throws IOException if an I/O error occurs
     */
    void commit() throws IOException;

    /**
     * Rolls back transaction, purging any newly written objects.
     * 
     * @throws IOException if an I/O error occurs
     */
    void rollback() throws IOException;

    /**
     * Ensure all data has hit disk and the transaction can be committed without
     * error. Used in two-phase commit.
     * 
     * Note: Once a transaction has been prepared it will not automatically be
     * committed or rolled-back. It MUST be explicitly rolled-back or
     * committed. Applications can recover lost transactions using 
     * {@link BlobStore.recover()}
     */
    void prepare();

}
