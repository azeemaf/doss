package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;

import org.junit.Test;

import java.util.Date;

import doss.Blob;
import doss.BlobTx;
import doss.DOSSTest;
import doss.local.Database.ContainerRecord;

public class ArchiverTest extends DOSSTest {

    @Test
    public void test() throws Exception {
        LocalBlobStore blobStore = (LocalBlobStore) this.blobStore;
        Database db = blobStore.db;
        long blobId1, blobId2, blobId3;

        // make sure we're using multiple directory levels
        db.increaseBlobIdSequence(1000);

        try (BlobTx tx = blobStore.begin()) {
            blobId1 = tx.put(TEST_BYTES).id();
            tx.commit();
        }

        // rolled back, should not be archived
        try (BlobTx tx = blobStore.begin()) {
            tx.put(TEST_BYTES).id();
            tx.rollback();
        }

        byte[] MORE_TEST_BYTES = new byte[TEST_BYTES.length * 2];
        System.arraycopy(TEST_BYTES, 0, MORE_TEST_BYTES, 0, TEST_BYTES.length);
        System.arraycopy(TEST_BYTES, 0, MORE_TEST_BYTES, TEST_BYTES.length, TEST_BYTES.length);
        try (BlobTx tx = blobStore.begin()) {
            blobId2 = tx.put(TEST_BYTES).id();
            blobId3 = tx.put(MORE_TEST_BYTES).id();
            tx.commit();
        }
        Archiver archiver = new Archiver(blobStore);

        assertTrue(Files.exists(blobStore.stagingPath(blobId1)));
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
            assertEquals(TEST_BYTES.length * 4, c.size());
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
            assertEquals(40, db.getContainerDigest(containerId,blobStore.getPreferredAlgorithm()).length());

            assertEquals(40, db.getDigest(blob.id(),blobStore.getPreferredAlgorithm()).length());
            assertEquals(40, db.getDigest(blobId3,blobStore.getPreferredAlgorithm()).length());
        }

        assertTrue(Files.exists(blobStore.stagingPath(blobId1)));

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

        // ensure staging blob files were deleted
        assertFalse(Files.exists(blobStore.stagingPath(blobId1)));

        // ensure parent directory which should now be empty is gone too
        assertFalse(Files.exists(blobStore.stagingPath(blobId1).getParent()));

        {
            Scrubber scrubber = new Scrubber(blobStore);
            scrubber.run();
        }
        assertEquals(true, db.getLastAuditResult(containerId));
        assertEquals(false, db.getLastAuditResult(99999999));
        assertNotNull(db.getLastAuditTime(containerId));
        Date lastAuditTime = db.getLastAuditTime(containerId);
        {
            Scrubber scrubber = new Scrubber(blobStore);
            scrubber.run();
            assertEquals(lastAuditTime, db.getLastAuditTime(containerId));
        }

        {
            Scrubber scrubber = new Scrubber(blobStore);
            scrubber.setSingleContainer(containerId);
            scrubber.run();
            assertNotEquals(lastAuditTime, db.getLastAuditTime(containerId));
        }
        {
            Scrubber scrubber = new Scrubber(blobStore);
            scrubber.setListFailedAudits(true);
            scrubber.run();
        }
    
    }
}
