package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;

import org.junit.Test;

public class BlobTest extends DOSSTest {
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
}
