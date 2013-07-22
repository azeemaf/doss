package doss.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import doss.NoSuchBlobException;
import doss.core.ContainerIndexWriter;

/**
 * Maintains an index for a {@link DirectoryContainer} such that a {@link Path}
 * object can be returned for any {@link Blob} stored in the
 * {@link DirectoryContainer}
 */
public class SymlinkContainerIndex implements ContainerIndexWriter {

    final private DirectoryContainer container;
    final private Path indexStorage;

    /**
     * Construct a new {@link SymlinkBlobIndex} for the given
     * {@link DirectoryContainer}
     * 
     * @param container
     *            The {@link DirectoryContainer} that will be indexed
     */
    SymlinkContainerIndex(final DirectoryContainer container,
            final Path indexStorage) {
        this.container = container;
        this.indexStorage = indexStorage;
    }

    @Override
    public void remember(long blobId, long offset) {
        Path link = resolveLinkPath(blobId);
        Path target = container.dataPathFor(offset);
        Path relativeTarget = link.getParent().relativize(target);
        try {
            Files.createSymbolicLink(link, relativeTarget);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(long blobId) {
        Path link = resolveLinkPath(blobId);
        if (!Files.exists(link)) {
            throw new NoSuchBlobException(Long.toString(blobId));
        }

        try {
            Files.delete(resolveLinkPath(blobId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Find a {@link Path} that can be used to retrieve the content for a
     * {@link Blob}
     * 
     * @param blobId
     *            The ID of the {@link Blob}
     * @return The {@link Path} where the {@link Blob} content can be found.
     */
    public Path contentPath(long blobId) {
        Path link = resolveLinkPath(blobId);
        if (!Files.exists(link, LinkOption.NOFOLLOW_LINKS)) {
            throw new NoSuchBlobException(Long.toString(blobId));
        }
        return link;
    }

    private Path resolveLinkPath(long blobId) {
        return indexStorage.resolve(Long.toString(blobId));
    }
}
