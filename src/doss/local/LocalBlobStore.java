package doss.local;

import java.io.IOException;
import java.nio.file.Path;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;

public class LocalBlobStore implements BlobStore {

    private final IdGenerator idGenerator;
    final Container container;
    final BlobIndex db = new MemoryBlobIndex();

    public LocalBlobStore(Path rootDir) throws IOException {
        idGenerator = new BruteForceIdGenerator(this);
        container = new DirectoryContainer(rootDir.resolve("data"));
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
        return new LocalBlobTx(this);
    }

    long generateBlobId() throws IOException {
        return idGenerator.generate();
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
