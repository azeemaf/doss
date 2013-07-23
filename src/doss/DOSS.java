package doss;

import java.io.IOException;
import java.nio.file.Path;

import org.skife.jdbi.v2.DBI;

import doss.core.BlobIndex;
import doss.local.LocalBlobStore;
import doss.sql.BlobIdDAO;
import doss.sql.BlobIdRunningNumber;
import doss.sql.BlobTxIdDAO;
import doss.sql.BlobTxIdRunningNumber;
import doss.sql.RunningNumber;
import doss.sql.SqlBlobIndex;

/**
 * Convenience methods for opening blob stores.
 */
public class DOSS {

    /**
     * Opens a BlobStore that stores all its data and indexes on the local file system.
     * @param root directory to store data and indexes in
     * @return a new BlobStore
     * @throws IOException if an I/O error occurs
     */
    public static BlobStore openLocalStore(Path root) throws IOException {
        DBI dbi = new DBI("jdbc:h2:file:" + root.resolve("index/index")+";AUTO_SERVER=TRUE");
        BlobIndex index = new SqlBlobIndex(dbi);

        DBI accountingDbi = new DBI("jdbc:h2:file:"
                + root.resolve("accounting/accounting") + ";AUTO_SERVER=TRUE");
        BlobIdDAO blobIdDao = accountingDbi.onDemand(BlobIdDAO.class);
        blobIdDao.createSchema();
        BlobTxIdDAO blobTxIdDao = accountingDbi.onDemand(BlobTxIdDAO.class);
        blobTxIdDao.createSchema();

        RunningNumber blobIdSource = new BlobIdRunningNumber(accountingDbi);
        RunningNumber blobTxIdSource = new BlobTxIdRunningNumber(accountingDbi);
        return new LocalBlobStore(root, index, blobIdSource, blobTxIdSource);
    }

}
