package doss.local;

import java.io.IOException;

import doss.local.Database.ContainerRecord;

/**
 * Admin tools. Should not be used by client applications. Potentially dangerous
 * operations and no interface stability guarantees.
 */
public class Admin {
    private final LocalBlobStore blobStore;

    public Admin(LocalBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public void listContainers() throws IOException {
        System.out.format("%8s %8s %12s %4s\n", "ID", "Size", "Area", "Sealed");
        for (ContainerRecord c : blobStore.db.findAllContainers()) {
            System.out.format("%8d %8d %12s %4s\n", c.id(), c.size(), c.area(),
                    c.sealed() ? "SEAL" : "");
        }
    }

    /**
     * Beware: only sets the flag in the database. Any active processes may keep
     * using the container.
     */
    public void sealContainer(long containerId) {
        blobStore.db.sealContainer(containerId);
    }

    public String locateBlob(long blobId) {
        BlobLocation loc = blobStore.db.locateBlob(blobId);
        return loc.toString();
    }
}
