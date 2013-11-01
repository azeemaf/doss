package doss.local;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import doss.core.WrappedBlobStore;

/**
 * Wrapper around LocalBlobStore that creates the blobstore in a temporary
 * directory and deletes it on close or JVM shutdown.
 */
public class TempBlobStore extends WrappedBlobStore {
    final Path root;
    final Thread shutdownHook = new Thread() {
        @Override
        public void run() {
            TempBlobStore.this.deleteRecursively();
        }
    };

    private TempBlobStore(Path root) {
        super(LocalBlobStore.open(root));
        this.root = root;
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public static TempBlobStore open() throws IOException {
        Path root = Files.createTempDirectory("doss-tempstore");
        LocalBlobStore.init(root);
        return new TempBlobStore(root);
    }

    @Override
    public void close() {
        super.close();
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

    public Path root() {
        return root;
    }
}
