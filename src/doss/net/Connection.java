package doss.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;

/**
 * Server's view of a DOSS connection.
 */
class Connection implements DossService.Iface, ServerContext {

    private final BlobStore blobStore;
    private final Map<Long, Upload> uploads = new HashMap<>();
    private long nextUploadId = 0;

    Connection(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public StatResponse stat(long blobId) throws TException {
        try {
            return statResponse(blobStore.get(blobId));
        } catch (NoSuchBlobException e) {
            throw new RemoteNoSuchBlobException().setBlobId(blobId);
        } catch (IOException e) {
            throw buildIOException(blobId, e);
        }
    }

    private StatResponse statResponse(Blob blob)
            throws IOException {
        return new StatResponse()
                .setBlobId(blob.id())
                .setCreatedMillis(blob.created().toMillis())
                .setSize(blob.size());
    }

    @Override
    public StatResponse statLegacy(String legacyPath)
            throws RemoteNoSuchBlobException, RemoteIOException, TException {
        try {
            return statResponse(blobStore.getLegacy(Paths.get(legacyPath)));
        } catch (NoSuchBlobException e) {
            throw new RemoteNoSuchBlobException();
        } catch (IOException e) {
            throw buildIOException(-1, e);
        }
    }

    @Override
    public ByteBuffer read(long blobId, long offset, int length)
            throws TException {
        try {
            ByteBuffer b = ByteBuffer.allocate(length);
            Blob blob = blobStore.get(blobId);
            if (offset > blob.size() || offset < 0) {
                throw new IOException("out of bounds read " + offset + " > "
                        + blob.size());
            }
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

    private RemoteIOException buildIOException(long blobId, Exception e) {
        return new RemoteIOException()
                .setBlobId(blobId)
                .setType(e.getClass().getName())
                .setMesssage(e.getMessage());
    }

    @Override
    public void commitTx(long txId) throws TException {
        try {
            blobStore.resume(txId).commit();
        } catch (NoSuchBlobTxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollbackTx(long txId) throws TException {
        try {
            abortUploads();
            blobStore.resume(txId).rollback();
        } catch (NoSuchBlobTxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void prepareTx(long txId) throws TException {
        try {
            blobStore.resume(txId).prepare();
        } catch (NoSuchBlobTxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Called when the client disconnects, cleanup any state.
     */
    public void disconnect() {
        System.out.println("Disconnected " + this);
        abortUploads();
    }

    private void abortUploads() {
        // cleanup any unfinished uploads
        for (Upload upload : uploads.values()) {
            upload.finish();
        }
    }

    @Override
    public long beginPut(long txId) throws TException {
        BlobTx tx = blobStore.resume(txId);
        long id = nextUploadId++;
        uploads.put(id, new Upload(tx));
        return id;
    }

    @Override
    public void write(long handle, ByteBuffer data) throws TException {
        uploads.get(handle).write(data);
    }

    @Override
    public long finishPut(long handle) throws TException {
        return uploads.remove(handle).finish();
    }

    @Override
    public String digest(long blobId, String algorithm) throws RemoteNoSuchBlobException, RemoteIOException, TException {
        try {
            return blobStore.get(blobId).digest(algorithm);
        } catch (NoSuchBlobException e) {
            throw new RemoteNoSuchBlobException().setBlobId(blobId);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw buildIOException(blobId, e);
        }
    }

    @Override
    public List<String> verify(long blobId) throws RemoteNoSuchBlobException, RemoteIOException, TException {
        try {
            return blobStore.get(blobId).verify();
        } catch (NoSuchBlobException e) {
            throw new RemoteNoSuchBlobException().setBlobId(blobId);
        } catch (IOException e) {
            throw buildIOException(blobId, e);
        }
    }
}