package doss;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class DOSSTest {
    static final String TEST_STRING = "test\nstring\0a\r\nwith\tstrange\u2603characters";
    static final byte[] TEST_BYTES = TEST_STRING.getBytes(Charset.forName("UTF-8"));
            
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private BlobStore blobStore;
    
    @Test(expected = NoSuchFileException.class)
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
    public void blobIdsShouldBeUnique() throws Exception {
        String id1 = writeTempBlob(blobStore, "one").id();
        String id2 = writeTempBlob(blobStore, "one").id();
        assertNotEquals(id1, id2);
    }
    
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
            byte[] buffer = new byte[TEST_STRING.getBytes().length];
            stream.read(buffer);
            assertEquals(TEST_STRING, new String(buffer));
        }
    }

    @Test(timeout = 1000)
    public void testStringIO() throws Exception {
        Blob blob;
        
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_STRING);
            tx.commit();
        }
        
        assertNotNull(blob.id());        
        assertEquals(TEST_STRING, blobStore.get(blob.id()).slurp());
    }
    
    @Test(expected = NoSuchFileException.class)
    public void testRollback() throws Exception {
        Blob blob;
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_STRING);
            tx.rollback();
        }
        
        blobStore.get(blob.id());
    }

    @Test(expected = NoSuchFileException.class)
    public void testImplicitRollback() throws Exception {
        Blob blob;
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_STRING);
        }
        
        blobStore.get(blob.id());
    }

    
    @Before
    public void openBlobStore() throws IOException {
        blobStore = DOSS.openLocalStore(folder.newFolder().toPath());
    }
    
    @After
    public void closeBlobStore() throws Exception {
        blobStore.close();
        blobStore = null;
    }

    private Blob writeTempBlob(BlobStore store, String testString) throws IOException, Exception {
        try (BlobTx tx = store.begin()) {
            Blob blob = tx.put(makeTempFile(testString));
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
