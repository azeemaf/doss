package doss;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import doss.core.Named;
import doss.core.Tools;

public class DOSSTest {
    static final String TEST_STRING = "test\nstring\0a\r\nwith\tstrange\u2603characters";
    static final byte[] TEST_BYTES = TEST_STRING.getBytes(Charset
            .forName("UTF-8"));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public BlobStore blobStore;

    @Before
    public void openBlobStore() throws IOException {
        Path root = folder.newFolder().toPath();
        Tools.createLocalStore(root);
        blobStore = DOSS.openLocalStore(root);
    }

    @After
    public void closeBlobStore() throws Exception {
        blobStore.close();
        blobStore = null;
    }

    /**
     * Reads the contents of the blob into a string (decodes with UTF-8).
     * 
     * @throws IOException
     *             if an I/O occurs
     */
    protected String slurp(Blob blob) throws IOException {
        byte[] buf = new byte[TEST_BYTES.length];
        try (SeekableByteChannel channel = blob.openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        return new String(buf, "UTF-8");
    }

    /**
     * Make a temporary file somewhere harmless with some test contents in it.
     * 
     * @returns Path to the temporary file
     * @throws IOException
     *             if an I/O occurs
     */
    protected Path makeTempFile(String contents) throws IOException {
        Path path = folder.newFile().toPath();
        Files.write(path, contents.getBytes());
        return path;
    }

    /**
     * Make a blob from a temporary file with the provided contents
     * 
     * @throws IOException
     *             if an I/O occurs
     */
    protected Named writeTempBlob(BlobStore store, String testString)
            throws IOException, Exception {
        try (BlobTx tx = store.begin()) {
            Named blob = tx.put(makeTempFile(testString));
            tx.commit();
            return blob;
        }
    }

}
