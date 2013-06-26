package doss.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import doss.Blob;
import doss.BlobTx;
import doss.output.ChannelOutput;

public class LocalBlobTx implements BlobTx {
    protected final LocalBlobStore blobStore;
    final List<LocalBlob> addedBlobs = new ArrayList<LocalBlob>();
    boolean committed;

    public LocalBlobTx(LocalBlobStore store) {
        this.blobStore = store;
    }

    @Override
    public void close() throws Exception {
        if (!committed) {
            rollback();
        }
    }

    @Override
    public void commit() {
        committed = true;
    }

    @Override
    public Blob put(Path source) throws IOException {
        String blobId = blobStore.generateBlobId();
        Files.copy(source, blobStore.pathFor(blobId));
        return new LocalBlob(blobStore, blobId);
    }

    @Override
    public Blob put(String contents) throws IOException {
        return put(contents.getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public Blob put(final byte[] bytes) throws IOException {
        return put(new ChannelOutput() {
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
            }
        });
    }

    @Override
    public Blob put(ChannelOutput output) throws IOException {
        LocalBlob blob = new LocalBlob(blobStore, blobStore.generateBlobId());
        
        try (WritableByteChannel channel = Files.newByteChannel(blob.getPath(),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            output.writeTo(channel);
        }
        
        addedBlobs.add(blob);
    
        return blob;
    }

    @Override
    public void rollback() throws IOException {
        for (LocalBlob blob: addedBlobs) {
            Files.delete(blob.getPath());
        }
    }

}
