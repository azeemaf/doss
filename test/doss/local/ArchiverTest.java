package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import doss.Blob;
import doss.BlobTx;
import doss.DOSSTest;
import doss.local.Database.ContainerRecord;

public class ArchiverTest extends DOSSTest {

    @Test
    public void test() throws Exception {
        Database db = ((LocalBlobStore) blobStore).db;
        long blobId1, blobId2, blobId3;
        try (BlobTx tx = blobStore.begin()) {
            blobId1 = tx.put(TEST_BYTES).id();
            blobId2 = tx.put(TEST_BYTES).id();
            blobId3 = tx.put(TEST_BYTES).id();
            tx.commit();
        }
        Archiver archiver = new Archiver(blobStore);

        assertEquals(3, db.findCommittedButUnassignedBlobs().size());
        {
            BlobLocation loc = db.locateBlob(blobId1);
            assertNull(loc.containerId());
            assertNull(loc.offset());
        }

        archiver.selectionPhase();

        long containerId;
        assertEquals(0, db.findCommittedButUnassignedBlobs().size());
        {
            Blob blob = blobStore.get(blobId1);
            assertEquals(TEST_STRING, slurp(blob));
            assertTrue(blob instanceof FileBlob);

            BlobLocation loc = db.locateBlob(blobId1);
            assertNotNull(loc.containerId());
            assertNull(loc.offset());
            containerId = loc.containerId();

            ContainerRecord c = db.findContainer(containerId);
            assertEquals(Database.CNT_OPEN, c.state());
            assertEquals(TEST_BYTES.length * 3, c.size());
        }

        int n = db.sealAllContainers();
        assertEquals(1, n);

        archiver.dataCopyPhase();

        {
            Blob blob = blobStore.get(blobId1);
            assertEquals(TEST_STRING, slurp(blob));

            BlobLocation loc = db.locateBlob(blobId1);
            assertEquals((Long) containerId, loc.containerId());
            assertNotNull(loc.offset());

            ContainerRecord c = db.findContainer(containerId);
            assertEquals(Database.CNT_WRITTEN, c.state());
            assertEquals(40, c.sha1().length());
        }

        archiver.cleanupPhase();

        {
            Blob blob = blobStore.get(blobId1);
            assertEquals(TEST_STRING, slurp(blob));
            assertEquals(CachedMetadataBlob.class, blob.getClass());

            BlobLocation loc = db.locateBlob(blobId1);
            assertEquals((Long) containerId, loc.containerId());
            assertNotNull(loc.offset());

            ContainerRecord c = db.findContainer(containerId);
            assertEquals(Database.CNT_ARCHIVED, c.state());
        }

    }
}
