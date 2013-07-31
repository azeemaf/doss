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
        assertInsertAndLocatable(1L, 2L, 3L);
        assertInsertAndLocatable(111111111111L, 222222222222L, 333333333333L);
        assertInsertAndLocatable(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
        assertInsertAndLocatable(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        assertLocatable(1L, 2L, 3L);
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
        db.insertBlob(1, 2, 3);
        assertNotNull(db.locateBlob(1));
        db.deleteBlob(1);
        assertNull(db.locateBlob(1));
    }

}
