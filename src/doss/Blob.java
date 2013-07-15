package doss;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;

/**
 * A named byte stream of known length that supports random read access.
 */
public interface Blob extends Named, Sized {

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
}
