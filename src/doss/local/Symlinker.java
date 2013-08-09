package doss.local;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import doss.NoSuchBlobException;

class Symlinker {
    enum FileExtension {
        JP2, XML, JPG, TIF
    }

    final private Path linkRoot;

    Symlinker(final Path indexStorage) {
        this.linkRoot = indexStorage;
    }

    public void link(long blobId, DirectoryContainer container, long offset) {
        link(blobId, container, offset, null);
    }

    public void link(long blobId, DirectoryContainer container, long offset,
            Path path) {
        Path link = resolveLinkPath(blobId, path);
        Path target = container.dataPathFor(offset);
        Path relativeTarget = link.getParent().relativize(target);
        try {
            Files.createSymbolicLink(link, relativeTarget);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void unlink(long blobId) throws NoSuchBlobException {
        Path link = resolveLinkPath(blobId, null);

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

    private Path resolveLinkPath(long blobId, Path path) {
        if (path == null)
            return linkRoot.resolve(Long.toString(blobId));

        // 8/8/13: a hack to enable iip srv to read jp2 image from Doss
        // which require a file extension to be added to the sym link
        // this will need be refactored later for the permernent solution
        // for serving files to 3rd party apps from Doss
        String ext = "";
        String fileName = path.toFile().getName();
        for (FileExtension type : FileExtension.values()) {
            String chkExt = "." + type.name().toLowerCase();
            if (fileName.endsWith(chkExt)) {
                ext = chkExt;
                break;
            }
        }
        return linkRoot.resolve(Long.toString(blobId) + ext);
    }
}
