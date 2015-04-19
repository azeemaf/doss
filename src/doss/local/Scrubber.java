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
    private final long DAY = 86400000;
    private long auditCutoff = DAY * 120; // how often to audit a container and its contents
    private String preferredAlgorithm;
    private int threads = 0;
    private int containerLimit = 1;
    private long singleContainer = 0;
    private boolean skipDbUpdate = false;
    private long showLastAudit = 0;

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

    public void run() throws IOException {
        if (showLastAudit > 0) {
            if (db.getLastAuditTime(showLastAudit) == null) {
                System.out.println("No audit result for Containter " + showLastAudit);
            } else {
                System.out.println("Last Audit for " + showLastAudit + " was " + db.getLastAuditTime(showLastAudit)
                    + " and the result was " + db.getLastAuditResult(showLastAudit));
                System.out.println("\tAlgorithm " + preferredAlgorithm + " Digest " + db.getContainerDigest(showLastAudit,preferredAlgorithm));
            }
        } else {
            if (singleContainer >0) {
                logger.info("Scrubber running in one shot mode (-i)");
                try {
                    if (db.findContainer(singleContainer).state() != Database.CNT_ARCHIVED) {
                        System.out.println("Only ARCHIVED containers can be verified");
                        throw new NoSuchContainerException(singleContainer);
                    }
                } catch (NullPointerException e) {
                    throw new NoSuchContainerException(singleContainer);
                }
                boolean result = verifyContainerAndContents(singleContainer);
                if (!skipDbUpdate) {
                    logger.info("Updating Audit Result for Container " + singleContainer + " to " + result);
                    synchronized (db) {
                        db.insertAuditResult(singleContainer,preferredAlgorithm,new java.util.Date(),result);
                    }
                } else {
                    logger.info("not storing Audit Result " + result + " for container " + singleContainer);
                }
            
            } else { 
                logger.info("Scrubber running in batch mode - " + containerLimit + " Container(s), " + auditCutoff/DAY + " days since last");
                verifyContainers();
            }
        }
    }

        // * For each ARCHIVED Container in all masterroots:
        // * verify every blob against db entry	
        // * flag errors at container level
    public void verifyContainers() throws IOException {
        List<Long> containerIds = db.findContainersByState(Database.CNT_ARCHIVED);
        logger.info("Verify phase: found " + containerIds.size() + " containers to scrub, verifying " + containerLimit);
        int loops = 0;
        for (long containerId : containerIds) {
            Date cutoff = new Date(System.currentTimeMillis() - auditCutoff);
            Date lastAuditTime = db.getLastAuditTime(containerId);
            if (lastAuditTime != null && lastAuditTime.after(cutoff)) {
                logger.info("Container " + containerId + " audited recently, skipping");
                continue;
            }
            boolean result = verifyContainerAndContents(containerId);
            if (!skipDbUpdate) {
                synchronized (db) {
                    db.insertAuditResult(containerId,preferredAlgorithm,new java.util.Date(),result);
                }
            } else {
                logger.info("not storing " + preferredAlgorithm + " Audit Result " + result + " for container " + containerId);
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
    private boolean verifyContainerAndContents(long containerId) throws IOException {
        for (Path fsRoot : this.blobStore.masterRoots) {
       	    Path tarPath =  blobStore.tarPath(fsRoot, containerId);
            if (Files.notExists(tarPath)) {
                logger.info("Container " + containerId + " does not exist at " + tarPath);
                return(false);
            }
        }
        for (Path fsRoot : this.blobStore.masterRoots) {
       	    Path tarPath =  blobStore.tarPath(fsRoot, containerId);
            logger.info("Verifying container " + containerId + " @ " + tarPath);
            // Do whole container verify first, streaming io/caching etc etc
            try (FileChannel tarChan = FileChannel.open(tarPath, READ)) {
                String digest = db.getContainerDigest(containerId,preferredAlgorithm);
                if (digest == null) {
                    logger.info("NEW digest for container " + containerId + " @ " + tarPath);
                    String containerDigest = Digests.calculate(preferredAlgorithm, tarChan);
                    digest = containerDigest;
                    if (!skipDbUpdate) {
                        synchronized (db) {
                            db.insertContainerDigest(containerId,preferredAlgorithm,containerDigest);
                        }
                    } else {
                        logger.info("not storing NEW digest " + preferredAlgorithm + " " + containerDigest + " for container " + containerId + " @ " + tarPath);
                    }
                    // no verify here, as digest has only just been created, it can be checked next time
                } else {
                    String containerDigest = Digests.calculate(preferredAlgorithm, tarChan);
                    if (!containerDigest.equals(digest)) {
                        logger.info("Verify failed for Container " + containerId
                            + " at " + tarPath + " using " + preferredAlgorithm
                            + ", expected " + digest + " but got " + containerDigest);
                        return(false);
                    } else {
                        logger.info("Verify passed for container " + containerId + " @ " + tarPath);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            // Now do each blob in the container
            try (TarContainer tar = new TarContainer(containerId, tarPath, FileChannel.open(
                tarPath, READ))) {
                logger.info("Verifying blobs in container " + containerId + " @ " + tarPath);
                Iterator<Blob> it = tar.iterator();
                while(it.hasNext()) {
                    Blob tarBlob = it.next();
                    String digest = db.getDigest(tarBlob.id(),preferredAlgorithm);
                    if (digest == null) {
                        String tarDigest = Digests.calculate(preferredAlgorithm, tarBlob);
                        if (!skipDbUpdate) {
                            synchronized (db) {
                                db.insertDigest(tarBlob.id(),preferredAlgorithm,tarDigest);
                            }
                        }
                        // no need to verify, digest is newly created
                    } else {
                        String tarDigest = Digests.calculate(preferredAlgorithm, tarBlob);
                        if (!digest.equals(tarDigest)) {
                            logger.info("Verify failed for blob " + tarBlob.id()
                                + " in container " + tarPath + " using " + preferredAlgorithm
                                + ", expected " + digest + " but got " + tarDigest);
                            return(false);
                        } else {
                            //logger.info("Verify for Blob " + tarBlob.id() + " in Container " + containerId + " successful");
                        }
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            logger.info("Verify blobs finished for container " + containerId + " @ " + tarPath);
        }
        return(true);
    }
    
    public void setShowLastAudit(long containerId) {
        this.showLastAudit = containerId;
    }
        
    // skip any database changes
    public void setSkipDbUpdate(boolean skip) {
        this.skipDbUpdate = skip;
    }

    public void setContainerLimit(int containers) {
        this.containerLimit = containers;
    }

    public void setSingleContainer(long containerId) {
        this.singleContainer = containerId;
    }

    public void setAuditCutoff(int days) {
        this.auditCutoff = this.DAY * days;
    }
        // multi threaded one day..
    public void setThreads(int threads) {
        this.threads = threads;
    }
}
