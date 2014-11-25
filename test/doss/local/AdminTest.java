package doss.local;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import doss.DOSSTest;
import doss.local.Database.ContainerRecord;

public class AdminTest extends DOSSTest {

    @Test
    public void ensureContainersCanBeSealed() throws Exception {
        LocalBlobStore blobStore = (LocalBlobStore) this.blobStore;
        Admin admin = new Admin(blobStore);
        long containerId = blobStore.db.createContainer();
        {
            ContainerRecord c = blobStore.db.findContainer(containerId);
            assertEquals(Database.CNT_OPEN, c.state());
        }
        admin.sealContainer(containerId);
        {
            ContainerRecord c = blobStore.db.findContainer(containerId);
            assertEquals(Database.CNT_SEALED, c.state());
        }
    }
}
