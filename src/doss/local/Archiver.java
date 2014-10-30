package doss.local;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import doss.BlobStore;
import doss.local.Database.ContainerRecord;

public class Archiver {

    private final static Logger logger = Logger.getLogger(Archiver.class
            .getName());
    private final LocalBlobStore blobStore;

    public Archiver(BlobStore blobStore) {
        if (!(blobStore instanceof LocalBlobStore)) {
            throw new IllegalArgumentException(
                    "archiver currently requires a local blobstore");
        }
        this.blobStore = (LocalBlobStore) blobStore;
    }

    public void run() {
        List<ContainerRecord> candidates = blobStore.db
                .findArchivalCandidates(blobStore.stagingArea.name());
        logger.info("Found " + candidates.size() + " sealed containers as archival candidates");
        for (ContainerRecord container : candidates) {
            if (blobStore.db.checkContainerForOpenTxs(container.id())) {
                logger.info("Container " + container.id()
                        + " still has open transactions, skipping it.");
                continue;
            }
            try {
                archive(container);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void archive(ContainerRecord container) throws IOException {
        Area master = blobStore.areas.get("area.master");
        if (master == null) {
            throw new IllegalStateException("No area.master configured in doss.conf");
        }
        logger.info("Moving container " + container.id() + " from "
                + blobStore.stagingArea.name() + " to " + master.name());
        master.moveContainerFrom(blobStore.stagingArea, container.id());
    }

}
