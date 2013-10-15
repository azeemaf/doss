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

import doss.local.LocalBlobStore;

public class DOSSTest {
    public static final String TEST_STRING = "test\nstring\0a\r\nwith\tstrange\u2603characters";
    public static final byte[] TEST_BYTES = TEST_STRING.getBytes(Charset
            .forName("UTF-8"));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public BlobStore blobStore;
    public Path blobStoreRoot;

    @Before
    public void openBlobStore() throws IOException {
        blobStoreRoot = folder.newFolder().toPath();
        LocalBlobStore.init(blobStoreRoot);
        blobStore = LocalBlobStore.open(blobStoreRoot);
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
    public static String slurp(Blob blob) throws IOException {
        try (SeekableByteChannel channel = blob.openChannel()) {
            byte[] buf = new byte[(int) channel.size()];
            channel.read(ByteBuffer.wrap(buf));
            return new String(buf, "UTF-8");
        }
    }

    /**
     * Make a blob from a temporary file with the provided contents
     * 
     * @throws IOException
     *             if an I/O occurs
     */
    public static Blob writeTempBlob(BlobStore store, String testString)
            throws IOException, Exception {
        try (BlobTx tx = store.begin()) {
            Blob blob = tx.put(testString.getBytes("UTF-8"));
            tx.commit();
            return blob;
        }
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
}
