package doss.local;

import org.junit.Test;

import doss.Blob;
import doss.BlobTx;
import doss.DOSSTest;

public class AdminTest extends DOSSTest {

    @Test(expected = ContainerInUseException.class)
    public void ensureContainersInUseCantBeSealed() throws Exception {
        LocalBlobStore blobStore = (LocalBlobStore) this.blobStore;
        Admin admin = new Admin(blobStore);
        try (BlobTx tx = blobStore.begin()) {
            Blob blob = tx.put(TEST_BYTES);
            BlobLocation loc = blobStore.db.locateBlob(blob.id());
            admin.sealContainer(loc.containerId());
            tx.rollback();
        }
    }

    @Test
    public void ensureContainersCanBeSealed() throws Exception {
        LocalBlobStore blobStore = (LocalBlobStore) this.blobStore;
        Admin admin = new Admin(blobStore);
        try (BlobTx tx = blobStore.begin()) {
            Blob blob = tx.put(TEST_BYTES);
            BlobLocation loc = blobStore.db.locateBlob(blob.id());
            tx.commit();
            admin.sealContainer(loc.containerId());
        }
    }
}
