package doss;

import java.io.IOException;

public interface Transaction extends AutoCloseable {
    /**
     * Commits the transaction.
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    void commit() throws IOException;

    /**
     * Rolls back transaction
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    void rollback() throws IOException;

    /**
     * Ensure all data has hit disk and the transaction can be committed without
     * error. Used in two-phase commit.
     * 
     */
    void prepare() throws IOException;

    /**
     * Closes the transaction handle and releases transaction management
     * resources.
     * 
     * @throws IOException
     * 
     */
    @Override
    void close() throws IOException;
}
