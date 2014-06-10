package doss.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.CorruptBlobStoreException;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;
import doss.Writable;
import doss.core.ManagedTransaction;
import doss.core.Transaction;
import doss.core.Writables;

public class LocalBlobStore implements BlobStore {

    final Symlinker symlinker;
    final Database db;
    final Map<Long, BlobTx> txs = new ConcurrentHashMap<>();
    final Path rootDir;
    final List<Area> areas = new ArrayList<>();
    final Area stagingArea;

    private LocalBlobStore(Path rootDir, String jdbcUrl) throws IOException {
        this.rootDir = rootDir;
        if (jdbcUrl == null) {
            db = Database.open(subdir("db"));
        } else {
            db = Database.open(jdbcUrl);
        }
        symlinker = new Symlinker(subdir("blob"));
        List<Filesystem> fslist = new ArrayList<>();
        fslist.add(new Filesystem("fs.staging", subdir("staging")));
        stagingArea = new Area(db, "area.staging", fslist, "directory");
        areas.add(stagingArea);
    }

    public static void init(Path root) throws IOException {
        Files.createDirectories(root.resolve("staging"));
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
            return new LocalBlobStore(root, null);
        } catch (IOException e) {
            throw new CorruptBlobStoreException(root, e);
        }
    }

    /**
     * Opens a BlobStore that stores all its data and indexes on the local file
     * system.
     * 
     * @param root
     *            directory to store data in
     * @param jdbcUrl
     *            jdbcUrl for the DOSS SQL database
     * @return a new BlobStore
     * @throws CorruptBlobStoreException
     *             if the blob store is missing or corrupt
     */
    public static BlobStore open(Path root, String jdbcUrl) throws CorruptBlobStoreException {
        try {
            return new LocalBlobStore(root, jdbcUrl);
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
        for (Area area : areas) {
            area.close();
        }
        db.close();
    }

    @Override
    public Blob get(long blobId) throws IOException, NoSuchBlobException {
        String legacyPath = db.locateLegacy(blobId);
        if (legacyPath != null) {
            return new FileBlob(blobId, Paths.get(legacyPath));
        }
        BlobLocation location = db.locateBlob(blobId);
        if (location == null) {
            throw new NoSuchBlobException(blobId);
        }

        // TODO: support multiple containers
        // This following call allow access to multiple containers
        return stagingArea.container(location.containerId()).get(
                location.offset());
    }

    @Override
    public Blob getLegacy(Path legacyPath) throws NoSuchBlobException,
            IOException {
        String path = legacyPath.toAbsolutePath().toString();
        if (!Files.exists(legacyPath)) {
            throw new NoSuchFileException(path);
        }
        return get(db.findOrInsertBlobIdByLegacyPath(path));
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

    public class Tx extends ManagedTransaction implements BlobTx {

        final long id = db.nextId();
        final List<Long> addedBlobs = new ArrayList<Long>();

        // ManagedTransaction will call back into this private Transaction when
        // the transaction changes state.
        // This allows us to have transaction state transition logic controlled
        // separately to the central data management concerns of this class.
        Transaction callbacks = new Transaction() {

            @Override
            public void commit() throws IOException {
                txs.remove(id);
            }

            @Override
            public void rollback() throws IOException {
                for (Long blobId : addedBlobs) {
                    db.deleteBlob(blobId);
                    symlinker.unlink(blobId);
                }
                txs.remove(id);
            }

            @Override
            public void prepare() {
                // TODO Auto-generated method stub
            }

            @Override
            public void close() throws IllegalStateException {
            }
        };

        public void setExtension(final long blobId, final String ext)
                throws NoSuchBlobException, IOException {
            Blob blob = get(blobId);
            symlinker.updateLinkPath(blobId, ext);
        }

        @Override
        public Blob put(final Path source) throws IOException {
            return put(Writables.wrap(source));
        }

        @Override
        public Blob put(final byte[] bytes) throws IOException {
            return put(Writables.wrap(bytes));
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        protected Transaction getCallbacks() {
            return callbacks;
        }

        @Override
        public Blob put(Writable output) throws IOException {
            state.assertOpen();
            db.begin();
            long blobId = db.nextId();
            Container container = stagingArea.currentContainer();
            long offset = container.put(blobId, output);
            db.insertBlob(blobId, container.id(), offset);
            if (container instanceof DirectoryContainer) {
                symlinker.link(blobId, (DirectoryContainer) container, offset);
            }
            addedBlobs.add(blobId);
            db.commit();
            return container.get(offset);
        }

        /**
         * Slightly dodgey addition to LocalBlobStore Tx, for importing legacy
         * files into a local DOSS. Files do not get a symlink as there are no
         * legacy jp2s.
         * 
         * @param legacyPath
         *            The full path to the legacy DOSS storage system.
         * 
         * @return The Blob id for the legacy file. File can now be retrieved
         *         just like any other DOSS stored file
         * 
         * @throws IOException
         *             when it's unhappy
         */
        public Long putLegacy(Path legacyPath) throws IOException {
            state.assertOpen();
            Long blobId = db.findOrInsertBlobIdByLegacyPath(legacyPath
                    .toString());
            addedBlobs.add(blobId);
            return blobId;
        }
    }
}
