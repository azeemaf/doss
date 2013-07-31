package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import doss.core.Named;

public class TransactionsTest extends DOSSTest {
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
        long txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
            tx.commit();
        }
        blobStore.resume(txId);
    }

    @Test
    public void preparedTransactionsStayOpen() throws Exception {
        long txId;
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
        long txId;
        try (BlobTx tx = blobStore.begin()) {
            txId = tx.id();
            tx.prepare();
            tx.rollback();
        }
        blobStore.resume(txId);
    }

    @Test(expected = NoSuchBlobTxException.class)
    public void preparedTransactionsAreClosedOnCommit() throws Exception {
        long txId;
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
