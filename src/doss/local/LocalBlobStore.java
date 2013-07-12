package doss.local;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.jdbcx.JdbcConnectionPool;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.logging.PrintStreamLog;
import org.skife.jdbi.v2.tweak.SQLLog;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobTxException;

public class LocalBlobStore implements BlobStore {

    final RunningNumber blobNumber = new RunningNumber();
    final RunningNumber txNumber = new RunningNumber();
    final Container container;
    final BlobIndex db;
    final Map<String, BlobTx> txs = new ConcurrentHashMap<>();

    public LocalBlobStore(Path rootDir) throws IOException {
        container = new DirectoryContainer(rootDir.resolve("data"));
        String connectionString = "jdbc:h2:file:" + rootDir.resolve("index/index")+";AUTO_SERVER=TRUE";
        JdbcConnectionPool ds = JdbcConnectionPool.create(connectionString,
                "username",
                "password");
        DBI dbi = new DBI(ds);
        SQLLog sqlLog = new PrintStreamLog(new PrintStream(System.out));
        dbi.setSQLLog(sqlLog);
        BlobIndexSchemaDAO schema = dbi.onDemand(BlobIndexSchemaDAO.class);
        schema.createSchema();
        Handle h = dbi.open();
        db = new SqlBlobIndex(h);

    }

    @Override
    public void close() throws Exception {
        container.close();
    }

    @Override
    public Blob get(String blobId) throws IOException {
        long offset = db.locate(parseId(blobId));
        return container.get(offset);
    }

    @Override
    public BlobTx begin() {
        BlobTx tx = new LocalBlobTx(Long.toString(txNumber.next()), this);
        txs.put(tx.id(), tx);
        return tx;
    }

    @Override
    public BlobTx resume(String txId) throws NoSuchBlobTxException {
        BlobTx tx = txs.get(txId);
        if (tx == null) {
            throw new NoSuchBlobTxException(txId);
        }
        return tx;
    }

    protected long parseId(String blobId) {
        try {
            return Long.parseLong(blobId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid blob id: "
                    + blobId, e);            
        }
    }
}
