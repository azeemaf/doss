package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;

import org.junit.Test;

import doss.local.LocalBlobStore;

public class BlobTest extends DOSSTest {
    @Test(expected = NoSuchBlobException.class)
    public void bogusBlobsShouldNotBeFound() throws Exception {
        blobStore.get(999);
    }

    @Test
    public void blobsHaveAUniqueId() throws Exception {
        long id1 = writeTempBlob(blobStore, "1").id();
        long id2 = writeTempBlob(blobStore, "2").id();
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
        LocalBlobStore.init(path);
        blobStore = LocalBlobStore.open(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertEquals(TEST_STRING, slurp(blobStore.get(blob.id())));
        blobStore.close();

        blobStore = LocalBlobStore.open(path);
        assertEquals(TEST_STRING, slurp(blobStore.get(blob.id())));
    }
}
