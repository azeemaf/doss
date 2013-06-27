package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import doss.Blob;

public class LocalBlob implements Blob {

    private final LocalBlobStore blobStore;
    private final String id;
    
    LocalBlob(LocalBlobStore blobStore, String id) {
        this.blobStore = blobStore;
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }
      
    Path getPath() {
        return blobStore.pathFor(id);
    }

    @Override
    public InputStream openStream() throws IOException {
        return Files.newInputStream(getPath());
    }

    @Override
    public SeekableByteChannel openChannel() throws IOException {
        return Files.newByteChannel(getPath(), StandardOpenOption.READ);
    }

    @Override
    public String slurp() throws IOException {
        return new String(Files.readAllBytes(getPath()), Charset.forName("UTF-8"));
    }
}
