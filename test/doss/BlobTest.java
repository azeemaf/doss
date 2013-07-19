package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BlobTest {
    static final String TEST_STRING = "test\nstring\0a\r\nwith\tstrange\u2603characters";
    static final byte[] TEST_BYTES = TEST_STRING.getBytes(Charset.forName("UTF-8"));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private BlobStore blobStore;
    
    @Before
    public void openBlobStore() throws IOException {
        blobStore = DOSS.openLocalStore(folder.newFolder().toPath());
    }

    @After
    public void closeBlobStore() throws Exception {
        blobStore.close();
        blobStore = null;
    }
    
    /*
     * Blobs
     */

    @Test(expected = NoSuchBlobException.class)
    public void bogusBlobsShouldNotBeFound() throws Exception {
        blobStore.get("999");
    }

    @Test(expected = IllegalArgumentException.class)
    public void pathHijinksShouldBeIllegal() throws Exception {
        blobStore.get("../secret");
    }

    @Test(expected = IllegalArgumentException.class)
    public void blankIdsShouldBeIllegal() throws Exception {
        blobStore.get("");
    }

    @Test
    public void blobsHaveAUniqueId() throws Exception {
        String id1 = writeTempBlob(blobStore, "one").id();
        String id2 = writeTempBlob(blobStore, "two").id();
        assertNotNull(id1);
        assertNotEquals(id1, id2);
    }

    @Test
    public void blobsHaveASize() throws Exception {
        try (BlobTx tx = blobStore.begin()) {
            assertEquals(TEST_BYTES.length, tx.put(TEST_BYTES).size());
            tx.rollback();
        }
    }

    @Test
    public void blobStoresReopenable() throws Exception {
        Path path = folder.newFolder().toPath();
        blobStore = DOSS.openLocalStore(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertEquals(TEST_STRING, slurp(blobStore.get(blob.id())));
        blobStore.close();

        blobStore = DOSS.openLocalStore(path);
        assertEquals(TEST_STRING, slurp(blobStore.get(blob.id())));
    }
    
    /**
     * Make a blob from a temporary file with the provided contents
     * 
     * @throws IOException if an I/O occurs
     */
    private Named writeTempBlob(BlobStore store, String testString)
            throws IOException, Exception {
        try (BlobTx tx = store.begin()) {
            Named blob = tx.put(makeTempFile(testString));
            tx.commit();
            return blob;
        }
    }

    /**
     * Make a temporary file somewhere harmless with some test contents in it.
     * 
     * @returns Path to the temporary file
     * @throws IOException if an I/O occurs
     */
    private Path makeTempFile(String contents) throws IOException {
        Path path = folder.newFile().toPath();
        Files.write(path, contents.getBytes());
        return path;
    }
    
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
}
