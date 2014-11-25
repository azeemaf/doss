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
        System.out.format("%8s %8s %10s\n", "ID", "Size", "State");
        for (ContainerRecord c : blobStore.db.findAllContainers()) {
            System.out.format("%8d %8d %10s\n", c.id(), c.size(), c.stateName());
        }
    }

    public void sealContainer(long containerId) throws ContainerInUseException {
        ContainerRecord c = blobStore.db.findContainer(containerId);
        if (c.state() != Database.CNT_OPEN) {
            throw new IllegalStateException("Container " + containerId
                    + " cannot be sealed from state " + c.stateName());
        }
        blobStore.db.updateContainerState(containerId, Database.CNT_SEALED);
    }

    public String locateBlob(long blobId) {
        BlobLocation loc = blobStore.db.locateBlob(blobId);
        return loc.toString();
    }
}
