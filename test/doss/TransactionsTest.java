package doss;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TransactionsTest {

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

}
