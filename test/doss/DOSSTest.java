package doss;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class DOSSTest {
    static final String TEST_STRING = "test\nstring\0a\r\nwith\tstrange\u2603characters";
    static final byte[] TEST_BYTES = TEST_STRING.getBytes(Charset.forName("UTF-8"));
            
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private BlobStore blobStore;
    
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
        }
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
    public void testStringIO() throws Exception {
        Named blob;
        
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_STRING);
            tx.commit();
        }
        
        assertNotNull(blob.id());        
        assertEquals(TEST_STRING, blobStore.get(blob.id()).slurp());
    }

    /*
     * Transactions
     */
    
    @Test(expected = NoSuchBlobException.class)
    public void testRollback() throws Exception {
        Named blob;
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_STRING);
            tx.rollback();
        }
        
        blobStore.get(blob.id());
    }

    @Test(expected = NoSuchBlobException.class)
    public void testImplicitRollback() throws Exception {
        Named blob;
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_STRING);
        }
        
        blobStore.get(blob.id());
    }

    @Test
    public void transactionsAreResumable() throws Exception {
        try (BlobTx tx = blobStore.begin()) {
            BlobTx tx2 = blobStore.resume(tx.id());
            assertEquals(tx, tx2);
        }
    }

    @Test(expected = NoSuchBlobTxException.class)
    public void closedTransactionsArentResumable() throws Exception {
        String txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
        }
        blobStore.resume(txId);
    }
    
    @Test
    public void preparedTransactionsStayOpen() throws Exception {
        String txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
            tx.prepare();
        }
        try (BlobTx tx = blobStore.resume(txId)) {
            tx.rollback();
        }
    }

    @Test(expected = NoSuchBlobTxException.class)
    public void preparedTransactionsAreClosedOnRollback() throws Exception {
        String txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
            tx.prepare();
            tx.rollback();
        }
        blobStore.resume(txId);
    }
    
    @Test(expected = NoSuchBlobTxException.class)
    public void preparedTransactionsAreClosedOnCommit() throws Exception {
        String txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
            tx.prepare();
            tx.commit();
        }
        blobStore.resume(txId);
    }
    
    @Test(expected = IllegalStateException.class)
    public void cantPutAfterCommit() throws Exception {
        try (BlobTx tx = blobStore.begin()) {
            tx.commit();
            tx.put(TEST_BYTES);
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void cantPutAfterRollback() throws Exception {
        try (BlobTx tx = blobStore.begin()) {
            tx.rollback();
            tx.put(TEST_BYTES);
        }
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

    private Named writeTempBlob(BlobStore store, String testString) throws IOException, Exception {
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
    
    
    /*
     * CLI
     */
    
    @Test
    public void cliGet() throws Exception {
        Path path = folder.newFolder().toPath();
        blobStore = DOSS.openLocalStore(path);
        
        Blob blob = null;
        
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        
        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);
        
        try {
            System.setOut(out);
            System.setProperty("doss.home", path.toString());
            Main.main("cat " + blob.id());
        } finally {
            System.setOut(oldOut);
        }
        
        assertEquals(TEST_STRING, outputStream.toString("UTF-8"));  
    }
    
}
