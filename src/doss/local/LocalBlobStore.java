package doss.local;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobTxException;
import doss.core.BlobIndex;
import doss.core.Container;
import doss.core.BlobIndexEntry;
import doss.sql.RunningNumber;

public class LocalBlobStore implements BlobStore {

    final RunningNumber blobNumber;
    final RunningNumber txNumber;
    final Container container;
    final BlobIndex blobIndex;
    final Map<String, BlobTx> txs = new ConcurrentHashMap<>();

    public LocalBlobStore(Path rootDir, BlobIndex index,
            RunningNumber blobIdSource, RunningNumber txIdSource)
            throws IOException {
        container = new DirectoryContainer(rootDir.resolve("data"));
        this.blobIndex = index;
        this.blobNumber = blobIdSource;
        this.txNumber = txIdSource;
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
