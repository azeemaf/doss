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

import doss.Blob;
import doss.BlobTx;
import doss.Writable;

public class LocalBlobTx implements BlobTx {
    final String id;
    final LocalBlobStore blobStore;
    final List<Long> addedBlobs = new ArrayList<Long>();
    boolean committed;

    public LocalBlobTx(String id, LocalBlobStore blobStore) {
        this.id = id;
        this.blobStore = blobStore;
    }

    @Override
    public void close() throws Exception {
        if (!committed) {
            rollback();
        }
        blobStore.txs.remove(id());
    }

    @Override
    public void commit() {
        committed = true;
    }

    @Override
    public Blob put(final Path source) throws IOException {
        return put(new Writable() {
            @Override
            public void writeTo(WritableByteChannel targetChannel) throws IOException {
                try (FileChannel sourceChannel = (FileChannel) Files.newByteChannel(source, StandardOpenOption.READ)) {
                    sourceChannel.transferTo(0, Long.MAX_VALUE, targetChannel);
                }
            }

            @Override
            public long size() throws IOException {
                return Files.size(source);
            }
        });
    }

    @Override
    public Blob put(String contents) throws IOException {
        return put(contents.getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public Blob put(final byte[] bytes) throws IOException {
        return put(new Writable() {
            @Override
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
            }

            @Override
            public long size() {
                return bytes.length;
            }
        });
    }

    @Override
    public Blob put(Writable output) throws IOException {
        long blobId = blobStore.blobNumber.next();
        long offset = blobStore.container.put(Long.toString(blobId), output);
        blobStore.db.remember(blobId, offset);
        addedBlobs.add(blobId);
        return blobStore.container.get(offset);
    }

    @Override
    public void rollback() throws IOException {
        for (Long blobId: addedBlobs) {
            blobStore.db.delete(blobId);
        }
    }

    @Override
    public String id() {
        return id;
    }

}
