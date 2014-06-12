package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import doss.Blob;

/**
 * A blob backed by a local file.
 */
class FileBlob implements Blob {

    private final Path path;
    private final long id;

    FileBlob(long id, Path path) {
        this.id = id;
        this.path = path;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public InputStream openStream() throws IOException {
        return Files.newInputStream(path);
    }

    @Override
    public SeekableByteChannel openChannel() throws IOException {
        return Files.newByteChannel(path, StandardOpenOption.READ);
    }

    @Override
    public long size() throws IOException {
        return Files.size(path);
    }

    @Override
    public FileTime created() throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class)
                .creationTime();
    }

    @Override
    public String digest(String algorithm) throws NoSuchAlgorithmException, IOException {
        try (SeekableByteChannel channel = openChannel()) {
            return Digests.calculate(algorithm, channel);
        }
    }

    @Override
    public List<String> verify() throws IOException {
        List<String> errors = new ArrayList<String>();
        if (!Files.exists(path)) {
            errors.add("missing from filesystem: " + path);
        } else if (!Files.isRegularFile(path)) {
            errors.add("not a regular file: " + path);
        } else if (!Files.isReadable(path)) {
            errors.add("not readable: " + path);
        }
        return errors;
    }
}
