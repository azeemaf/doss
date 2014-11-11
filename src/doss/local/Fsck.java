package doss.local;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import doss.local.Database.ContainerRecord;

/**
 * Filesystem check tool.
 *
 * Attempts to detect problems such as:
 *
 * 1. Orphaned files in staging.
 * 2. Files in the wrong dir in staging.
 * 3. Unreadable files.
 * 4. Files with the wrong name.
 * 5. Blobs recorded in db without corresponding staging files.
 * 6. Orphaned containers.
 * 7. Containers in db with missing tar files.
 * 8. Blobs marked as archived but with files still in staging. (failed cleanup)
 *
 */
public class Fsck {

    private final LocalBlobStore blobStore;
    private final Database db;
    private boolean verbose = false;

    public Fsck(LocalBlobStore blobStore) {
        this.blobStore = blobStore;
        this.db = blobStore.db;
    }

    public Fsck setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public void run() throws IOException {
        checkStagingFs();
        for (Path fsRoot : blobStore.masterRoots) {
            checkMasterFs(fsRoot);
        }
        checkDatabase();
    }

    void report(Path file, String message) {
        System.out.println(file + " " + message);
    }

    void checkStagingFs() throws IOException {
        Files.walkFileTree(blobStore.stagingRoot.resolve("data"), new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                if (!Files.isDirectory(file)) {
                    String filename = file.getFileName().toString();
                    if (filename.startsWith("nla.blob-")) {
                        long blobId;
                        try {
                            blobId = blobStore.parseBlobFileName(filename);
                        } catch (NumberFormatException e) {
                            report(file, "bad filename");
                            return FileVisitResult.CONTINUE;
                        }
                        BlobLocation loc = db.locateBlob(blobId);
                        if (loc == null) {
                            report(file, "orphaned file");
                        } else if (!loc.isInStagingArea()) {
                            report(file,
                                    "should not exist, blob archived. Container cleanup failed?");
                        } else if (!file.equals(blobStore.stagingPath(blobId))) {
                            report(file,
                                    "in wrong location. Should be " + blobStore.stagingPath(blobId));
                        } else if (!Files.isReadable(file)) {
                            report(file, "not readable");
                        } else if (loc.containerId() != null && loc.containerState() == null) {
                            report(file, "has container id nla.doss-" + loc.containerId()
                                    + " but container state is null");
                        } else if (loc.txId() == null) {
                            report(file, "has null tx_id");
                        } else if (verbose) {
                            report(file, "OK");
                        }
                    } else {
                        report(file, "unexpected file");
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                report(file, exc.toString());
                return FileVisitResult.CONTINUE;
            }

        });
    }

    void checkMasterFs(final Path fsRoot) throws IOException {
        Files.walkFileTree(fsRoot.resolve("data"), new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                if (!Files.isDirectory(file)) {
                    String filename = file.getFileName().toString();
                    if (filename.startsWith("nla.doss-")) {
                        long containerId;
                        try {
                            containerId = blobStore.parseContinerFileName(filename);
                        } catch (NumberFormatException e) {
                            report(file, "bad filename");
                            return FileVisitResult.CONTINUE;
                        }
                        ContainerRecord cnt = db.findContainer(containerId);
                        if (cnt == null) {
                            report(file, "orphaned container");
                        } else if (cnt.state() != Database.CNT_ARCHIVED) {
                            report(file,
                                    "should not exist, container is in state: " + cnt.stateName());
                        } else if (!file.equals(blobStore.tarPath(fsRoot, containerId))) {
                            report(file,
                                    "in wrong location. Should be "
                                            + blobStore.tarPath(fsRoot, containerId));
                        } else if (!Files.isReadable(file)) {
                            report(file, "not readable");
                        } else if (verbose) {
                            report(file, "OK");
                        }
                    } else {
                        report(file, "unexpected file");
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                report(file, exc.toString());
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void checkDatabase() {
        for (BlobLocation loc : db.locateAllBlobs()) {
            if (loc.isInStagingArea()) {
                Path file = blobStore.stagingPath(loc.blobId());
                if (!Files.isReadable(file) || !Files.isRegularFile(file)) {
                    report(file, "missing or unreadable");
                }
            }
        }
        for (ContainerRecord cnt : db.findAllContainers()) {
            if (cnt.state() == Database.CNT_ARCHIVED) {
                for (Path fsRoot : blobStore.masterRoots) {
                    Path file = blobStore.tarPath(fsRoot, cnt.id());
                    if (!Files.isReadable(file) || !Files.isRegularFile(file)) {
                        report(file, "missing or unreadable");
                    }
                }
            }
        }
    }
}
