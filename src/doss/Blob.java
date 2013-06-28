package doss;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;

public interface Blob {
    /**
     * Gets the storage identifier for this blob.
     * 
     * @return the storage identifier
     */
    public String id();
    
    /**
     * Opens the blob for reading and returns an InputStream.
     * 
     * @return an InputStream for reading from the blob
     * @throws IOException if an I/O error occurs
     */
    public InputStream openStream() throws IOException;

    /**
     * Opens the blob for reading a returns a seekable byte channel.
     * 
     * @return a channel for reading from the blob
     * @throws IOException if an I/O occurs
     */
    public SeekableByteChannel openChannel() throws IOException;

    
    /**
     * Reads the contents of the blob into a string (decodes with UTF-8).
     * 
     * @return the contents of the blob
     * @throws IOException if an I/O error occurs
     */
    public String slurp() throws IOException;
}
