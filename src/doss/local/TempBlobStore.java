package doss.local;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;

/**
 * Wrapper around LocalBlobStore that creates the blobstore in a temporary
 * directory and deletes it on close or JVM shutdown.
 */
public class TempBlobStore implements BlobStore {
    final BlobStore wrapped;
    final Path root;
    final Thread shutdownHook = new Thread() {
        @Override
        public void run() {
            TempBlobStore.this.deleteRecursively();
        }
    };

    public TempBlobStore() throws IOException {
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        root = Files.createTempDirectory("doss-tempstore");
        LocalBlobStore.init(root);
        wrapped = LocalBlobStore.open(root);
    }

    @Override
    public Blob get(long blobId) throws NoSuchBlobException, IOException {
        return wrapped.get(blobId);
    }

    @Override
    public BlobTx begin() {
        return wrapped.begin();
    }

    @Override
    public BlobTx resume(long txId) throws NoSuchBlobTxException {
        return wrapped.resume(txId);
    }

    @Override
    public void close() {
        wrapped.close();
        deleteRecursively();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    private void deleteRecursively() {
        if (!Files.exists(root)) {
            return;
        }
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult postVisitDirectory(Path dir,
                        IOException exc) throws IOException {
                    Files.deleteIfExists(dir);
                    return super.postVisitDirectory(dir, exc);
                }

                @Override
                public FileVisitResult visitFile(Path file,
                        BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
