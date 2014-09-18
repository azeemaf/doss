package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

public class DatabaseTest {
    static Database db;

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        db = Database.open(folder.newFolder().toPath());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        db.close();
    }

    @Before
    public void setUp() throws Exception {
        db.migrate();
    }

    @After
    public void tearDown() throws Exception {
        db.getHandle().execute("DROP ALL OBJECTS");
    }

    @Test
    public void testNextId() {
        assertNotEquals("ids should be unique", db.nextId(), db.nextId());
    }

    @Test
    public void testInsertBlob() {
        long containedId = db.createContainer("staging");
        assertInsertAndLocatable(1L, containedId, 3L);
        assertInsertAndLocatable(111111111111L, containedId, 333333333333L);
        assertInsertAndLocatable(Long.MIN_VALUE, containedId, Long.MIN_VALUE);
        assertInsertAndLocatable(Long.MAX_VALUE, containedId, Long.MAX_VALUE);
        assertLocatable(1L, containedId, 3L);
    }

    private void assertInsertAndLocatable(long blobId, long containerId,
            long offset) {
        assertNull(db.locateBlob(blobId));
        db.insertBlob(blobId, containerId, offset);
        assertLocatable(blobId, containerId, offset);
    }

    private void assertLocatable(long blobId, long containerId, long offset) {
        BlobLocation location = db.locateBlob(blobId);
        assertNotNull(location);
        assertEquals("container id", containerId, location.containerId());
        assertEquals("offset", offset, location.offset());
    }

    @Test
    public void testDeleteBlob() {
        long containedId = db.createContainer("staging");
        db.insertBlob(1, containedId, 3);
        assertNotNull(db.locateBlob(1));
        db.deleteBlob(1);
        assertNull(db.locateBlob(1));
    }

    @Test
    public void testMultipleOpenContainers() {
        String area = "unit-test";
        Long firstId = db.createContainer(area);
        assertEquals(firstId, db.findAnOpenContainer(area));
        db.createContainer(area);
        db.createContainer(area);
        assertEquals(firstId, db.findAnOpenContainer(area));
    }

    @Test
    public void testDigests() {
        db.insertDigest(1, "sha1", "test");
        db.insertDigest(1, "md5", "test2");
        db.insertDigest(2, "sha1", "test3");
        assertEquals("test", db.getDigest(1, "sha1"));
        assertEquals("test2", db.getDigest(1, "md5"));
        assertEquals("test3", db.getDigest(2, "sha1"));
    }

    @Test(expected = UnableToExecuteStatementException.class)
    public void testDuplicateDigests() {
        db.insertDigest(1, "sha1", "test");
        db.insertDigest(1, "sha1", "test");
    }
}
