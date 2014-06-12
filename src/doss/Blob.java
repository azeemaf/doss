package doss;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import doss.core.Named;
import doss.core.Sized;

/**
 * A named byte stream of known length that supports random read access.
 */
public interface Blob extends Named, Sized {

    /**
     * Opens the blob for reading and returns an InputStream.
     * 
     * @return an InputStream for reading from the blob
     * @throws IOException
     *             if an I/O error occurs
     */
    public InputStream openStream() throws IOException;

    /**
     * Opens the blob for reading a returns a seekable byte channel.
     * 
     * @return a channel for reading from the blob
     * @throws IOException
     *             if an I/O error occurs
     */
    public SeekableByteChannel openChannel() throws IOException;

    /**
     * Returns the blob's creation date.
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    public FileTime created() throws IOException;

    /**
     * Returns the stored digest of this blob.
     * 
     * @param algorithm
     *            digest algorithm to use (eg "SHA1", "MD5")
     * @return a hex-encoded digest
     * @throws NoSuchAlgorithmException
     *             if the given digest algorithm is unavailable
     * @throws IOException
     *             if an I/O error occurs
     */
    public String digest(String algorithm) throws NoSuchAlgorithmException, IOException;

    /**
     * Performs an integrity check of this blob. Compares the various (database,
     * filesystem, caches etc.) copies of information about this Blob with each
     * other.
     * 
     * @return a human readable list of integrity errors
     * @throws IOException
     *             if an I/O error occurs
     */
    public List<String> verify() throws IOException;
}
