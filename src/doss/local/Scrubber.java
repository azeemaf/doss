package doss.local;

import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.Date;

import doss.Blob;
import doss.BlobStore;

public class Scrubber {

    private final static Logger logger = Logger.getLogger(Scrubber.class.getName());
    private final LocalBlobStore blobStore;
    private final Database db;
    private final long AuditCutoff = 86400000 * 120; // how often to audit a container and its contents
    private boolean skipDbUpdate;
    private String preferredAlgorithm;
    private int threads = 0;
    private boolean verifyOk;
    private int containerLimit;

    public Scrubber(BlobStore blobStore) {
        if (!(blobStore instanceof LocalBlobStore)) {
            throw new IllegalArgumentException(
                    "scrubber currently requires a local blobstore");
        }
        this.blobStore = (LocalBlobStore) blobStore;
        db = this.blobStore.db;
        preferredAlgorithm = this.blobStore.getPreferredAlgorithm();
        if (this.blobStore.masterRoots.isEmpty()) {
            throw new IllegalArgumentException(
                    "scrubber can only be run on a blobstore with at least one master filesystem configured");
        }
    }

    public void run(boolean skipDbUpdate) throws IOException {
        logger.info("Scrubber running..");
        verifyPhase();
    }

        // * For each ARCHIVED Container in all masterroots:
        // * verify every blob against db entry	
        // * flag errors at container level
    public void verifyPhase() throws IOException {
        List<Long> containerIds = db.findContainersByState(Database.CNT_ARCHIVED);
        logger.info("Verify phase: found " + containerIds.size() + " containers to scrub");
        int loops = 0;
        for (long containerId : containerIds) {
            Date cutoff = new Date(System.currentTimeMillis() - AuditCutoff);
            Date lastAuditTime = db.getLastAuditTime(containerId);
            if (lastAuditTime != null && lastAuditTime.before(cutoff)) {
                logger.info("Container audited recently, skipping");
                continue;
            }
            verifyOk = true;
            verifyContainerAndContents(containerId);
            if (!skipDbUpdate) {
                logger.info("Updating Audit Result for Container " + containerId + " to " + verifyOk);
                synchronized (db) {
                    db.insertAuditResult(containerId,preferredAlgorithm,new java.util.Date(),verifyOk);
                }
           }
            loops++;
            if (loops >= containerLimit) {
                break;
            } 
        }
    }

        // * foreach master area - given a containerId
        // * verify container digest with db
        // * loop through container and  verify all blob digests with db
        // * do it by container because once the offline one is staged,
        // * its best to continue with that container
        // * skip any blobs that have been checked recently
    private void verifyContainerAndContents(long containerId) throws IOException {
        for (Path fsRoot : this.blobStore.masterRoots) {
       	    Path tarPath =  blobStore.tarPath(fsRoot, containerId);
            if (Files.notExists(tarPath)) {
                logger.info("Container " + containerId + " does not exist at " + tarPath);
                continue;
            }
            logger.info("Checking container " + containerId + " @ " + tarPath);
            // Do whole container verify first, streaming io/caching etc etc
            try (FileChannel tarChan = FileChannel.open(tarPath, READ)) {
                String digest = db.getContainerDigest(containerId,preferredAlgorithm);
                if (digest == null) {
                    logger.info("No DB Digest for container " + containerId + ", adding it");
                    String containerDigest = Digests.calculate(preferredAlgorithm, tarChan);
                    digest = containerDigest;
                    if (!skipDbUpdate) {
                        synchronized (db) {
                            db.insertContainerDigest(containerId,preferredAlgorithm,containerDigest);
                        }
                    }
                    // no verify here, as digest has only just been created, it can be checked next time
                } else {
                    logger.info("Container " + containerId + " : algorithm " + preferredAlgorithm + " and DB digest " + digest);
                    logger.info("Calculating " + preferredAlgorithm + " digest for container " + containerId + " at " + tarPath);
                    String containerDigest = Digests.calculate(preferredAlgorithm, tarChan);
                    if (!containerDigest.equals(digest)) {
                        verifyOk = false;
                        throw new IOException("verify failed for container " + containerId
                            + " at " + tarPath + " using " + preferredAlgorithm
                            + ", expected " + digest + " but got " + containerDigest);
                    } else {
                        logger.info("Verify for Container " + containerId + " in " + fsRoot + " successful");
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            // Now do each blob in the container
            try (TarContainer tar = new TarContainer(containerId, tarPath, FileChannel.open(
                tarPath, READ))) {
                logger.info("Scrubbing container at " + tarPath);
                Iterator<Blob> it = tar.iterator();
                while(it.hasNext()) {
                    Blob tarBlob = it.next();
                    String digest = db.getDigest(tarBlob.id(),preferredAlgorithm);
                    if (digest == null) {
                        logger.info("No DB Digest for " + tarBlob.id() + " using " + preferredAlgorithm + ", adding it");
                        String tarDigest = Digests.calculate(preferredAlgorithm, tarBlob);
                        if (!skipDbUpdate) {
                            synchronized (db) {
                                db.insertDigest(tarBlob.id(),preferredAlgorithm,tarDigest);
                            }
                        }
                        // no need to verify, digest is newly created
                    } else {
                        logger.info("Got DB Digest " + digest + " using " + preferredAlgorithm);
                        String tarDigest = Digests.calculate(preferredAlgorithm, tarBlob);
                        logger.info("Got Tar Digest " + digest + " using " + preferredAlgorithm);
                        if (!digest.equals(tarDigest)) {
                            verifyOk = false;
                            throw new IOException("verify failed for blob " + tarBlob.id()
                                + " in container " + tarPath + " using " + preferredAlgorithm
                                + ", expected " + digest + " but got " + tarDigest);
                        } else {
                            logger.info("Verify for Blob " + tarBlob.id() + " in Container " + containerId + " successful");
                        }
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setContainerLimit(int containers) {
        this.containerLimit = containers;
    }

        // multi threaded one day..
    public void setThreads(int threads) {
        this.threads = threads;
    }
}
