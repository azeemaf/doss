package doss.local;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import doss.Blob;
import doss.BlobStore;
import doss.core.Writables;

public class Archiver {

    private final static Logger logger = Logger.getLogger(Archiver.class
            .getName());
    private final LocalBlobStore blobStore;
    private final Database db;
    private final long maxContainerSize = 10L * 1024 * 1024 * 1024;

    public Archiver(BlobStore blobStore) {
        if (!(blobStore instanceof LocalBlobStore)) {
            throw new IllegalArgumentException(
                    "archiver currently requires a local blobstore");
        }
        this.blobStore = (LocalBlobStore) blobStore;
        db = this.blobStore.db;
        if (this.blobStore.masterRoots.isEmpty()) {
            throw new IllegalArgumentException(
                    "archiver can only be run on a blobstore with at least one master filesystem configured");
        }
    }

    public void run(boolean sealImmediately) throws IOException {
        selectionPhase();
        if (sealImmediately) {
            int n = db.sealAllContainers();
            logger.info("Force sealed " + n + " containers");
        }
        dataCopyPhase();
        cleanupPhase();
    }

    //      * For each blob:
    //        * If blob is part of an uncommitted transaction, skip it.
    //        * If blob is already part of a container, skip it.
    //        * Find existing or create container state="open"
    //        * Add container_id, blob_id to container_blobs table.
    //        * If container_size > THRESHOLD:
    //          * Update current_container set state="selected"
    public void selectionPhase() throws IOException {
        List<Long> blobIds = db.findCommittedButUnassignedBlobs();
        logger.info("Selection phase: found " + blobIds.size() + " candidate blobs for archiving");
        for (long blobId : blobIds) {
            Blob blob = blobStore.get(blobId);
            Long containerId = db.findAnOpenContainer();
            if (containerId == null) {
                containerId = db.createContainer();
            }
            db.addBlobToContainer(blobId, containerId, blob.size());
            long containerSize = db.getContainerSize(containerId);
            if (containerSize > maxContainerSize) {
                db.updateContainerState(containerId, Database.CNT_SEALED);
            }
        }
    }

    Path incomingPath(Path fsRoot, long containerId) {
        return fsRoot.resolve("incoming").resolve("nla.doss-" + containerId + ".tar");
    }

    //      * For each container where state="selected":
    //        * Create container file in incoming dir each fs (overwrite and truncate if already exists - crash cleanup)
    //        * Add each blob to tar file
    //        * fsync tar file
    //        * Calculate digest from each fs (if not matching: delete file and retry)
    //        * Update container set state="written"
    //
    public void dataCopyPhase() throws IOException {
        List<Long> containerIds = db.findContainersByState(Database.CNT_SEALED);
        logger.info("Data copy phase: found " + containerIds.size()
                + " full containers ready to be written");
        for (long containerId : containerIds) {
            List<TarContainer> tars = new ArrayList<>();

            logger.info("Writing container " + containerId);
            try {
                for (Path fsRoot : blobStore.masterRoots) {
                    Path tarPath = incomingPath(fsRoot, containerId);
                    tars.add(new TarContainer(containerId, tarPath, FileChannel.open(
                            tarPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)));
                }

                List<Long> blobIds = db.findBlobsByContainer(containerId);
                logger.info("Writing " + blobIds.size() + " blobs to container " + containerId);
                for (long blobId : blobIds) {
                    Blob blob = blobStore.get(blobId);
                    Long offset = null;
                    for (TarContainer container : tars) {
                        logger.fine("Appending blob " + blobId + " (" + blob.size() + " bytes) to "
                                + container.path());
                        offset = container.put(blobId, Writables.wrap(blob));
                    }
                    db.setBlobOffset(blobId, offset);
                }
            } finally {
                for (Container tar : tars) {
                    tar.fsync();
                    tar.close();
                }
            }

            // TODO: verify

            for (Path fsRoot : blobStore.masterRoots) {
                Path dest = blobStore.tarPath(fsRoot, containerId);
                Files.createDirectories(dest.getParent());
                Files.move(incomingPath(fsRoot, containerId), dest, StandardCopyOption.ATOMIC_MOVE);
            }
            db.updateContainerState(containerId, Database.CNT_WRITTEN);
        }
    }

    //      * For each container where state="written":
    //        * For each blob in container:
    //          * Delete blob from staging area.
    //        * Update container set state="archived"

    public void cleanupPhase() throws IOException {
        List<Long> containerIds = db.findContainersByState(Database.CNT_WRITTEN);
        logger.info("Cleanup phase: found " + containerIds.size()
                + " written containers ready to be cleaned up");
        for (long containerId : containerIds) {
            List<Long> blobIds = db.findBlobsByContainer(containerId);
            for (long blobId : blobIds) {
                Path blobPath = blobStore.stagingPath(blobId);
                logger.info("Deleting " + blobPath);
                Files.deleteIfExists(blobPath);
            }
            db.updateContainerState(containerId, Database.CNT_ARCHIVED);
        }
    }
}
