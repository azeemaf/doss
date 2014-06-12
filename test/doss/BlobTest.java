package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.file.NoSuchFileException;
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
    public void blobsHaveADigest() throws Exception {
        try (BlobTx tx = blobStore.begin()) {
            assertEquals("91613d0dc2a18f748962a86024ac620a0cc10919", tx.put(TEST_BYTES).digest("SHA1"));
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

    @Test
    public void legacyBlobsKeepTheirId() throws Exception {
        Path path1 = folder.newFile().toPath();
        Path path2 = folder.newFile().toPath();
        Blob b = blobStore.getLegacy(path1);
        Blob b2 = blobStore.getLegacy(path1);
        assertEquals(b.id(), b2.id());
        Blob b3 = blobStore.getLegacy(path2);
        assertNotEquals(b.id(), b3.id());
    }

    @Test(expected = NoSuchFileException.class)
    public void legacyPathsMustExist() throws Exception {
        blobStore.getLegacy(folder.getRoot().toPath().resolve("doesnotexist"));
    }
}
