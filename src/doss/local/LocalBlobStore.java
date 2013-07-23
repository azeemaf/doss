package doss.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.skife.jdbi.v2.DBI;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobTxException;
import doss.core.BlobIndex;
import doss.core.BlobIndexEntry;
import doss.sql.SqlBlobIndex;

public class LocalBlobStore implements BlobStore {

    public static LocalBlobStore open(Path root) throws IOException {
        Path dataStorage = root.resolve("data");
        assertStorageLocationExists(dataStorage, "DirectoryContainer storage");
        DirectoryContainer container = new DirectoryContainer(dataStorage);

        Path sqlIndexStorage = root.resolve("index/index");
        assertStorageLocationExists(dataStorage, "SqlContainerIndex storage");
        DBI dbi = new DBI("jdbc:h2:file:" + sqlIndexStorage
                + ";AUTO_SERVER=TRUE");
        SqlBlobIndex sqlIndex = new SqlBlobIndex(dbi);

        Path symlinkRoot = root.resolve("blob");
        assertStorageLocationExists(dataStorage,
                "SymlinkContainerIndex storage");

        Symlinker symlinker = new Symlinker(symlinkRoot);

        return new LocalBlobStore(container, sqlIndex, symlinker);
    }

    private static void assertStorageLocationExists(Path storageLocation,
            String storageDescription) {
        if (!Files.exists(storageLocation)) {
            throw new RuntimeException(storageDescription + " not found");
        }
    }

    final RunningNumber blobNumber = new RunningNumber();
    final Map<String, BlobTx> txs = new ConcurrentHashMap<>();
    final DirectoryContainer container;
    final BlobIndex blobIndex;
    final Symlinker symlinker;

    private final RunningNumber txNumber = new RunningNumber();

    public LocalBlobStore(DirectoryContainer container, BlobIndex index,
            Symlinker symlinker) throws IOException {
        this.container = container;
        this.blobIndex = index;
        this.symlinker = symlinker;
    }

    @Override
    public void close() {
        container.close();
    }

    @Override
    public Blob get(String blobId) throws IOException {
        BlobIndexEntry entry = blobIndex.locate(parseId(blobId));
        return container.get(entry.offset());
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
