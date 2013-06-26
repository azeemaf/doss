package doss.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import doss.BlobTx;
import doss.output.ChannelOutput;

public class LocalBlobTx implements BlobTx {
    protected final LocalBlobStore blobStore;
    final List<Long> addedBlobs = new ArrayList<Long>();
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
    public String put(final Path source) throws IOException {
        return put(new ChannelOutput() {
            public void writeTo(WritableByteChannel targetChannel) throws IOException {
                try (FileChannel sourceChannel = (FileChannel) Files.newByteChannel(source, StandardOpenOption.READ)) {
                    sourceChannel.transferTo(0, Long.MAX_VALUE, targetChannel);
                }
            }
        });
    }

    @Override
    public String put(String contents) throws IOException {
        return put(contents.getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public String put(final byte[] bytes) throws IOException {
        return put(new ChannelOutput() {
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
            }
        });
    }

    @Override
    public String put(ChannelOutput output) throws IOException {
        long blobId = blobStore.generateBlobId();
        long offset = blobStore.container.put(Long.toString(blobId), output);
        blobStore.db.remember(blobId, offset);
        addedBlobs.add(blobId);
        return Long.toString(blobId);
    }

    @Override
    public void rollback() throws IOException {
        for (Long blobId: addedBlobs) {
            blobStore.db.delete(blobId);
        }
    }

}
