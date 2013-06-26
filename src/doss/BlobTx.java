package doss;

import java.io.IOException;
import java.nio.file.Path;

import doss.output.ChannelOutput;

public interface BlobTx extends AutoCloseable {

    /**
     * Store a new blob.
     * 
     * @param channelOutput something that can be written to a byte channel
     * @return the new blob
     * @throws IOException if an I/O error occurs
     */
    Blob put(ChannelOutput output) throws IOException;
    
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

}
