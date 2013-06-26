package doss.output;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * An object that can be written to a byte channel.
 */
public interface ChannelOutput {

    /**
     * Writes this output to a byte channel.
     * 
     * @param channel the channel to write to
     * @throws IOException if I/O error occurrs
     */
    void writeTo(WritableByteChannel channel) throws IOException;

}
