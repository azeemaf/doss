package doss;

import java.io.IOException;
import java.nio.file.Path;

import doss.core.Named;
import doss.core.Transaction;
import doss.core.TransactionState;

/**
 * A transaction for writing to a BlobStore.
 */
public interface BlobTx extends Named, Transaction {

    /**
     * Store a new blob.
     * 
     * @param channelOutput
     *            something that can be written to a byte channel
     * @return the new blob
     * @throws IOException
     *             if an I/O error occurs
     */
    Blob put(Writable output) throws IOException;

    /**
     * Store a local file as a new blob.
     * 
     * @param source
     *            path to the file to store
     * @return the new blob
     * @throws IOException
     *             if an I/O error occurs
     */
    Blob put(Path source) throws IOException;

    /**
     * Store the contents of a byte array as a new blob.
     * 
     * @param bytes
     *            bytes to store
     * @return the new blob
     * @throws IOException
     *             if an I/O error occurs
     */
    Blob put(byte[] bytes) throws IOException;

    /**
     * Commits the transaction, ensuring all blobs written in this transaction
     * are persisted in permanent storage.
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    void commit() throws IllegalStateException, IOException;

    /**
     * Rolls back transaction, purging any newly written objects.
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    void rollback() throws IllegalStateException, IOException;

    /**
     * Ensure all data has hit disk and the transaction can be committed without
     * error. Used in two-phase commit.
     * 
     * Note: Once a transaction has been prepared it will not automatically be
     * committed or rolled-back. It MUST be explicitly rolled-back or committed.
     * Applications can recover lost transactions using {@link
     * BlobStore.recover()}
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    void prepare() throws IllegalStateException, IOException;

    /**
     * Closes the transaction handle and releases resources. Normal operation
     * does not alter the transaction state, but see below for exceptions.
     * 
     * This method should only be called after calling {@link #prepare()},
     * {@link #commit()} or {@link #rollback()}. If this method is called while
     * the transaction is in any other state, then an
     * {@link IllegalStateException} will be thrown and the transaction will be
     * rolled back.
     * 
     * Implementors should look at {@link TransactionState} for how to
     * get this behaviour.
     * 
     * @throws IllegalStateException
     *             If called while the transaction is not in a valid state to be
     *             closed. The transaction will be automatically rolled back
     *             when this exception is thrown.
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    void close() throws IllegalStateException, IOException;
}
