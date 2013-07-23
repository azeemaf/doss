package doss;

import java.io.IOException;
import java.nio.file.Path;

import doss.local.LocalBlobStore;

/**
 * Convenience methods for opening blob stores.
 */
public class DOSS {

    /**
     * Opens a BlobStore that stores all its data and indexes on the local file
     * system.
     * 
     * @param root
     *            directory to store data and indexes in
     * @return a new BlobStore
     * @throws IOException
     *             if an I/O error occurs
     */
    public static BlobStore openLocalStore(Path root) throws IOException {
        return LocalBlobStore.open(root);
    }

}
