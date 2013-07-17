package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import doss.Blob;

public class LocalBlob implements Blob {

    private final Path path;
    private final String id;
    
    LocalBlob(String id, Path path) {
        this.id = id;
        this.path = path;
    }

    @Override
    public String id() {
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
        return Files.readAttributes(path, BasicFileAttributes.class).creationTime();
    }

    @Override
    public FileTime lastModifed() throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class).lastModifiedTime();
    }
    
    @Override
    public FileTime lastAccess() throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class).lastAccessTime();
    }
}
