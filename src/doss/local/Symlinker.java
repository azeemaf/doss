package doss.local;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import doss.NoSuchBlobException;

class Symlinker {
    final private Path linkRoot;
    final private boolean enabled;

    Symlinker(final Path indexStorage) {
        this.linkRoot = indexStorage;
        enabled = !System.getProperty("os.name").startsWith("Windows");
    }

    public void link(long blobId, DirectoryContainer container, long offset) {
        if (!enabled)
            return;
        Path link = resolveLinkPath(blobId);
        Path target = container.dataPathFor(offset);
        Path relativeTarget = link.getParent().relativize(target);
        try {
            Files.createSymbolicLink(link, relativeTarget);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void unlink(long blobId) throws NoSuchBlobException {
        if (!enabled)
            return;
        Path link = resolveLinkPath(blobId);

        // 8/8/13: check for any sym link created as blobId and/or
        // blobId with an file extension
        try (DirectoryStream<Path> matches = Files.newDirectoryStream(linkRoot,
                "" + blobId + ".*")) {
            boolean hasMatches = false;
            for (Path entry : matches) {
                hasMatches = true;
                Files.delete(entry);
            }

            if (!hasMatches && !Files.exists(link)) {
                throw new NoSuchBlobException(blobId);
            }

            if (Files.exists(link)) {
                Files.delete(link);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateLinkPath(long blobId, String mimeExt) {
        Path fromLinkPath = linkRoot.resolve(Long.toString(blobId) + ".jp2");
        Path toLinkPath = linkRoot.resolve(Long.toString(blobId) + mimeExt);
        fromLinkPath.toFile().renameTo(toLinkPath.toFile());
    }

    private Path resolveLinkPath(long blobId) {
        // 8/8/13: a hack to enable iip srv to read jp2 image from Doss
        // which require a file extension to be added to the sym link
        // this will need be refactored later for the permanent solution
        // for serving files to 3rd party apps from Doss
        return linkRoot.resolve(Long.toString(blobId) + ".jp2");
    }
}
