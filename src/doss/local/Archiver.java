package doss.local;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
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
    private boolean skipCleanup = false;
    private int threads = 0;

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
        if (!skipCleanup) {
            cleanupPhase();
        }
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
        if (threads == 0) {
            for (long containerId : containerIds) {
                writeContainer(containerId);
            }
        } else {
            parallelDataCopyPhase(containerIds);
        }
    }

    private void parallelDataCopyPhase(List<Long> containerIds) {
        logger.info("Running parallel data copy with " + threads + " threads");
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (final long containerId : containerIds) {
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        writeContainer(containerId);
                        return null;
                    }
                });
            }
            threadPool.invokeAll(tasks);
        } catch (InterruptedException e) {
            throw new RuntimeException("parallel data copy interrupted", e);
        } finally {
            threadPool.shutdown();
        }
    }

    private void writeContainer(long containerId) throws IOException {
        List<TarContainer> tars = new ArrayList<>();

        logger.info("Writing container " + containerId);
        try {
            for (Path fsRoot : blobStore.masterRoots) {
                Path tarPath = incomingPath(fsRoot, containerId);
                Files.deleteIfExists(tarPath); // remove any debris from a crashed previous attempt
                FileChannel channel = FileChannel.open(tarPath, CREATE_NEW, WRITE);
                tars.add(new TarContainer(containerId, tarPath, channel));
            }

            List<Long> blobIds;
            synchronized (db) {
                blobIds = db.findBlobsByContainer(containerId);
            }
            logger.info("Writing " + blobIds.size() + " blobs to container " + containerId);
            for (long blobId : blobIds) {
                Blob blob;
                synchronized (db) {
                    blob = blobStore.get(blobId);
                }
                Long offset = null;
                for (TarContainer container : tars) {
                    logger.fine("Appending blob " + blobId + " (" + blob.size() + " bytes) to "
                            + container.path());
                    offset = container.put(blobId, Writables.wrap(blob));
                }
                synchronized (db) {
                    db.setBlobOffset(blobId, offset);
                }
            }
        } finally {
            for (Container tar : tars) {
                tar.fsync();
                tar.close();
            }
        }

        // verify all blobs exist and have the right contents
        for (Path fsRoot : blobStore.masterRoots) {
            verifyContainerContents(containerId, fsRoot);
        }

        // calculate whole container digests and compare to each other
        String lastDigest = null;
        for (Path fsRoot : blobStore.masterRoots) {
            Path path = incomingPath(fsRoot, containerId);
            logger.info("Calculating digest for " + path);
            try (FileChannel chan = FileChannel.open(path, READ)) {
                String digest = Digests.calculate("SHA1", chan);
                if (lastDigest != null && !digest.equals(lastDigest)) {
                    throw new IOException("tar digests do not match. Expected " + lastDigest
                            + " but got " + digest + " for " + path);
                }
                lastDigest = digest;
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        synchronized (db) {
            db.insertContainerDigest(containerId,"SHA1",lastDigest);

        }
        // all ok, do the final move
        for (Path fsRoot : blobStore.masterRoots) {
            Path dest = blobStore.tarPath(fsRoot, containerId);
            Files.createDirectories(dest.getParent());
            Files.move(incomingPath(fsRoot, containerId), dest, StandardCopyOption.ATOMIC_MOVE);
        }
        synchronized (db) {
            db.updateContainerState(containerId, Database.CNT_WRITTEN);
        }
        logger.info("Finished data copy for container " + containerId);
    }

    private void verifyContainerContents(long containerId, Path fsRoot) throws IOException {
        Path tarPath = incomingPath(fsRoot, containerId);
        logger.info("Verifying individual records in " + tarPath);
        try (TarContainer tar = new TarContainer(containerId, tarPath, FileChannel.open(
                tarPath, READ))) {
            Iterator<Blob> it = tar.iterator();
            List<Long> blobIds;
            synchronized (db) {
                blobIds = db.findBlobsByContainer(containerId);
            }
            for (long blobId : blobIds) {
                Blob stagingBlob;
                synchronized (db) {
                    stagingBlob = blobStore.get(blobId);
                }
                if (!it.hasNext()) {
                    throw new IOException("tar ended prematurely");
                }
                Blob tarBlob = it.next();
                if (stagingBlob.id() != tarBlob.id()) {
                    throw new IOException("expected blob " + stagingBlob.id()
                            + " but found " + tarBlob.id());
                }
                if (stagingBlob.size() != tarBlob.size()) {
                    throw new IOException("blob " + tarBlob.id() + " expected size "
                            + stagingBlob.size()
                            + " but found " + tarBlob.size());
                }
                String tarDigest = Digests.calculate("SHA1", tarBlob);
                String stagingDigest = stagingBlob.digest("SHA1");
                if (!tarDigest.equals(stagingDigest)) {
                    throw new IOException("copy verify failed for blob " + tarBlob.id()
                            + " expected SHA1 " + stagingDigest + " but tar contains " + tarDigest);
                }
            }
            if (it.hasNext()) {
                throw new IOException(tarPath + " has more records than expected");
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
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
                deleteEmptyDirs(blobPath.getParent(), blobStore.stagingRoot.resolve("data"));
            }
            db.updateContainerState(containerId, Database.CNT_ARCHIVED);
        }
    }

    /**
     * Walk upwards from path to root deleting empty directories.
     */
    private void deleteEmptyDirs(Path path, Path root) throws IOException {
        try {
            while (path.startsWith(root) && !path.equals(root) && Files.isDirectory(path)) {
                Files.deleteIfExists(path);
                path = path.getParent();
            }
        } catch (DirectoryNotEmptyException e) {
            // we're done
        }
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setSkipCleanup(boolean skipCleanup) {
        this.skipCleanup = skipCleanup;
    }
}
