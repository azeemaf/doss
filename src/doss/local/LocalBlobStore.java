package doss.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import doss.*;
import doss.core.ManagedTransaction;
import doss.core.Transaction;
import doss.core.Writables;

public class LocalBlobStore implements BlobStore {

    final DirectoryContainer container;
    final Symlinker symlinker;
    final Database db;
    final Map<Long, BlobTx> txs = new ConcurrentHashMap<>();
    final Path rootDir;

    private LocalBlobStore(Path rootDir) throws IOException {
        this.rootDir = rootDir;
        container = new DirectoryContainer(0, subdir("data"));
        db = Database.open(subdir("db"));
        symlinker = new Symlinker(subdir("blob"));
    }

    public static void init(Path root) throws IOException {
        Files.createDirectories(root.resolve("data"));
        Files.createDirectories(root.resolve("db"));
        Files.createDirectories(root.resolve("blob"));
        try (Database db = Database.open(root.resolve("db"))) {
            db.migrate();
        }
    }
    
    /**
     * Opens a BlobStore that stores all its data and indexes on the local file
     * system.
     * 
     * @param root
     *            directory to store data and indexes in
     * @return a new BlobStore
     * @throws CorruptBlobStoreException
     *             if the blob store is missing or corrupt
     */
    public static BlobStore open(Path root) throws CorruptBlobStoreException {
        try {
            return new LocalBlobStore(root);
        } catch (IOException e) {
            throw new CorruptBlobStoreException(root, e);
        }
    }

    private Path subdir(String name) throws NotDirectoryException {
        Path path = rootDir.resolve(name);
        if (!Files.isDirectory(path)) {
            throw new NotDirectoryException(path.toString());
        }
        return path;
    }

    @Override
    public void close() {
        container.close();
    }

    @Override
    public Blob get(long blobId) throws IOException, NoSuchBlobException {
        BlobLocation location = db.locateBlob(blobId);
        if (location == null) {
            throw new NoSuchBlobException(blobId);
        }
        // TODO: support multiple containers
        return container.get(location.offset());
    }

    @Override
    public BlobTx begin() {
        BlobTx tx = new Tx();
        txs.put(tx.id(), tx);
        return tx;
    }

    @Override
    public BlobTx resume(long txId) throws NoSuchBlobTxException {
        BlobTx tx = txs.get(txId);
        if (tx == null) {
            throw new NoSuchBlobTxException(txId);
        }
        return tx;
    }
    
    protected class Tx extends ManagedTransaction implements BlobTx {
        
        final long id = db.nextBlobTxId();
        final List<Long> addedBlobs = new ArrayList<Long>();

        // ManagedTransaction will call back into this private Transaction when
        // the transaction changes state.
        // This allows us to have transaction state transition logic controlled
        // separately to the central data management concerns of this class.
        Transaction callbacks = new Transaction() {
            public void commit() throws IOException {
                txs.remove(id);
            }

            public void rollback() throws IOException {
                for (Long blobId : addedBlobs) {
                    db.deleteBlob(blobId);
                    symlinker.unlink(blobId);
                }
                txs.remove(id);
            }

            public void prepare() {
                // TODO Auto-generated method stub
            }

            public void close() throws IllegalStateException {
            }
        };

        public synchronized Blob put(Writable output) throws IOException {
            state.assertOpen();
            long blobId = db.nextBlobId();
            long offset = container.put(blobId, output);
            db.insertBlob(blobId, container.id(), offset);
            symlinker.link(blobId, container, offset);
            addedBlobs.add(blobId);
            return container.get(offset);
        }

        public Blob put(final Path source) throws IOException {
            return put(Writables.wrap(source));
        }

        public Blob put(final byte[] bytes) throws IOException {
            return put(Writables.wrap(bytes));
        }

        public long id() {
            return id;
        }

        protected Transaction getCallbacks() {
            return callbacks;
        }
    }
}
