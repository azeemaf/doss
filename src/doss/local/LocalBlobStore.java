package doss.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.IdGenerator;

public class LocalBlobStore implements BlobStore {

    private final Path rootDir, dataDir;
    private final IdGenerator idGenerator;

    public LocalBlobStore(Path rootDir) throws IOException {
        this.rootDir = rootDir;

        idGenerator = new BruteForceIdGenerator(this);
        dataDir = rootDir.resolve("data");
        Files.createDirectory(dataDir);
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public LocalBlob get(String blobId) throws NoSuchFileException {
        Path path = pathFor(blobId);
        if (Files.exists(path)) {
            return new LocalBlob(this, blobId);
        } else {
            throw new NoSuchFileException(path.toString());
        }
    }

    @Override
    public BlobTx begin() {
        return new LocalBlobTx(this);
    }

    String generateBlobId() {
        return idGenerator.generate();
    }

    Path pathFor(String blobId) {
        validateBlobId(blobId);
        return dataDir.resolve(blobId);
    }

    protected void validateBlobId(String blobId) {
        try {
            Long.parseLong(blobId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid blob id: "
                    + blobId, e);            
        }
    }
}
