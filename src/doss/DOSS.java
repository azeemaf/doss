package doss;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.skife.jdbi.v2.DBI;

import doss.core.ContainerIndex;
import doss.core.ContainerIndexWriter;
import doss.core.ContainerIndexWriterProxy;
import doss.local.DirectoryContainer;
import doss.local.LocalBlobStore;
import doss.local.SymlinkContainerIndex;
import doss.sql.SqlContainerIndex;

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
