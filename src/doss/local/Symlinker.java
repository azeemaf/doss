package doss.local;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import doss.NoSuchBlobException;

class Symlinker {

    final private Path linkRoot;

    Symlinker(final Path indexStorage) {
        this.linkRoot = indexStorage;
    }

    public void link(long blobId, DirectoryContainer container, long offset) {
        Path link = resolveLinkPath(blobId);
        Path target = container.dataPathFor(offset);
        Path relativeTarget = link.getParent().relativize(target);
        try {
            Files.createSymbolicLink(link, relativeTarget);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void unlink(long blobId) {
        Path link = resolveLinkPath(blobId);
        if (!Files.exists(link)) {
            throw new NoSuchBlobException(blobId);
        }

        try {
            Files.delete(resolveLinkPath(blobId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path resolveLinkPath(long blobId) {
        // 8/8/13: a hack to enable iip srv to read jp2 image from Doss
        // will need refactoring later for the proper solution
        // for serving files to 3rd party apps from Doss
        return linkRoot.resolve(Long.toString(blobId) + ".jp2");
    }
}
