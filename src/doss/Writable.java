package doss;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * An object that can be written to a byte channel.
 */
public interface Writable {

    /**
     * Writes this object to a byte channel.
     *
     * @param channel
     *            the channel to write to
     * @throws IOException
     *             if I/O error occurs
     * @return bytes written
     */
    long writeTo(WritableByteChannel channel) throws IOException;

}
