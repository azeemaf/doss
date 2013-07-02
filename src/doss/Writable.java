package doss;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * An object of known size that can be written to a byte channel.
 */
public interface Writable extends Sized {

    /**
     * Writes this object to a byte channel.
     * 
     * @param channel the channel to write to
     * @throws IOException if I/O error occurrs
     */
    void writeTo(WritableByteChannel channel) throws IOException;
    
}
