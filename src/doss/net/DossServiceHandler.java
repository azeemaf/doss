package doss.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import org.apache.thrift.TException;

import doss.Blob;
import doss.BlobStore;
import doss.NoSuchBlobException;

class DossServiceHandler implements DossService.Iface {

    private final BlobStore blobStore;

    DossServiceHandler(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public StatResponse stat(long blobId) throws TException {
        try {
            Blob blob = blobStore.get(blobId);
            return new StatResponse(blobId, blob.size());
        } catch (NoSuchBlobException e) {
            throw new RemoteNoSuchBlobException().setBlobId(blobId);
        } catch (IOException e) {
            throw buildIOException(blobId, e);
        }
    }

    @Override
    public ByteBuffer read(long blobId, long offset, int length)
            throws TException {
        try {
            ByteBuffer b = ByteBuffer.allocate(length);
            Blob blob = blobStore.get(blobId);
            try (SeekableByteChannel channel = blob.openChannel()) {
                channel.position(offset);
                channel.read(b);
            }
            b.flip();
            if (b.remaining() < length) {
                throw new IOException("read requested " + length
                        + " bytes but only received " + b.remaining());
            }
            return b;
        } catch (NoSuchBlobException e) {
            throw new RemoteNoSuchBlobException().setBlobId(blobId);
        } catch (IOException e) {
            throw buildIOException(blobId, e);
        }
    }

    @Override
    public long beginTx() throws TException {
        return blobStore.begin().id();
    }

    private RemoteIOException buildIOException(long blobId, IOException e) {
        return new RemoteIOException()
                .setBlobId(blobId)
                .setType(e.getClass().getName())
                .setMesssage(e.getMessage());
    }
}