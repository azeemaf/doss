package doss;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class DOSSTest {
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
     * Transactions
     */

    @Test(expected = NoSuchBlobException.class)
    public void testRollback() throws Exception {
        Named blob;
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.rollback();
        }

        blobStore.get(blob.id());
    }

    @Test(expected = NoSuchBlobException.class)
    public void testImplicitRollback() throws Exception {
        Named blob = null;
        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
        } catch (IllegalStateException e) {
            // ignore - not part of test
        }
        assertNotNull("Got a blob", blob);
        blobStore.get(blob.id());
    }

    @Test
    public void transactionsAreResumable() throws Exception {
        try (BlobTx tx = blobStore.begin()) {
            BlobTx tx2 = blobStore.resume(tx.id());
            assertEquals(tx, tx2);
            tx2.rollback();
        }
    }

    @Test(expected = NoSuchBlobTxException.class)
    public void closedTransactionsArentResumable() throws Exception {
        String txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
            tx.commit();
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

    /*
     * CLI
     */

    @Test
    public void cliCat() throws Exception {
        Path path = folder.newFolder().toPath();
        blobStore = DOSS.openLocalStore(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertNotNull("Blob is not null", blob);
        
        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setOut(out);
            System.setProperty("doss.home", path.toString());
            Main.main("cat", blob.id());
        } finally {
            System.setOut(oldOut);
        }

        assertEquals(TEST_STRING, outputStream.toString("UTF-8"));
    }
    
    @Test
    public void cliPut() throws Exception {
        Path dossPath = folder.newFolder().toPath();
        
        Path tempFilePath = makeTempFile(TEST_STRING);
        
        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);
        
        try {
            System.setOut(out);
            System.setProperty("doss.home", dossPath.toString());
            Main.main("put", tempFilePath.toString());
        } finally {
            System.setOut(oldOut);
        }
        
        assertTrue(outputStream.toString("UTF-8").contains(tempFilePath.getFileName().toString()));
        assertTrue(outputStream.toString("UTF-8").contains("Created 1 blobs."));
        
        //first column is digits and is a blobID.
        Pattern idPattern = Pattern.compile("\\n(\\d*)\\t");
        Matcher m = idPattern.matcher(outputStream.toString("UTF-8"));
        String id = null;
        while (m.find()) {
            id = m.group(1);
        }
        
        blobStore = DOSS.openLocalStore(dossPath);
        
        byte[] buf = new byte[TEST_BYTES.length];
        try (SeekableByteChannel channel = blobStore.get(id).openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        assertEquals(TEST_STRING, new String(buf, "UTF-8"));
    }
    
    @Test
    public void cliPutBogusFile() throws Exception {
        Path dossPath = folder.newFolder().toPath();
                
        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);
        
        try {
            System.setErr(out);
            System.setProperty("doss.home", dossPath.toString());
            Main.main("put", "this/file/should/probably/not/exist");
        } finally {
            System.setErr(oldOut);
        }
        
        assertTrue(outputStream.toString("UTF-8").contains("no such file"));
    }

    @Test
    public void cliInfo() throws Exception {
        Path path = folder.newFolder().toPath();
        blobStore = DOSS.openLocalStore(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertNotNull("Blob is not null", blob);
        
        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setOut(out);
            System.setProperty("doss.home", path.toString());
            Main.main("stat", blob.id());
        } finally {
            System.setOut(oldOut);
        }

        assertTrue(outputStream.toString().contains(blob.id()));
        assertTrue(outputStream.toString().contains("Created"));
        assertTrue(outputStream.toString().contains(Integer.toString(TEST_BYTES.length)));
    }
}
