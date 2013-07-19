package doss;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class IOTest {
    static final String TEST_STRING = "test\nstring\0a\r\nwith\tstrange\u2603characters";
    static final byte[] TEST_BYTES = TEST_STRING.getBytes(Charset.forName("UTF-8"));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private BlobStore blobStore;
    
    /**
     * Reads the contents of the blob into a string (decodes with UTF-8).
     * 
     * @throws IOException if an I/O occurs
     */
    private String slurp(Blob blob) throws IOException {
        byte[] buf = new byte[TEST_BYTES.length];
        try (SeekableByteChannel channel = blob.openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        return new String(buf, "UTF-8");
    }
    
    /*
     * I/O
     */

    @Test(timeout = 1000)
    public void testChannelIO() throws Exception {
        String blobId;

        try (BlobTx tx = blobStore.begin()) {
            blobId = tx.put(TEST_BYTES).id();
            tx.commit();
        }

        byte[] buf = new byte[TEST_BYTES.length];
        try (SeekableByteChannel channel = blobStore.get(blobId).openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        assertEquals(TEST_STRING, new String(buf, "UTF-8"));
    }

    @Test(timeout = 1000)
    public void testStreamIO() throws Exception {
        String id = writeTempBlob(blobStore, TEST_STRING).id();
        assertNotNull(id);

        Blob blob = blobStore.get(id);
        try (InputStream stream = blob.openStream()) {
            byte[] buffer = new byte[TEST_STRING.getBytes("UTF-8").length];
            stream.read(buffer);
            assertEquals(TEST_STRING, new String(buffer, "UTF-8"));
        }
    }

    @Test(timeout = 1000)
    public void testBytesIO() throws Exception {
        Named blob;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        
        assertNotNull(blob.id());
        assertEquals(TEST_STRING, slurp(blobStore.get(blob.id())));
    }

    /*
     * Misc
     */

    @Before
    public void openBlobStore() throws IOException {
        blobStore = DOSS.openLocalStore(folder.newFolder().toPath());
    }

    @After
    public void closeBlobStore() throws Exception {
        blobStore.close();
        blobStore = null;
    }

    private Named writeTempBlob(BlobStore store, String testString)
            throws IOException, Exception {
        try (BlobTx tx = store.begin()) {
            Named blob = tx.put(makeTempFile(testString));
            tx.commit();
            return blob;
        }
    }

    private Path makeTempFile(String contents) throws IOException {
        Path path = folder.newFile().toPath();
        Files.write(path, contents.getBytes());
        return path;
    }

}
