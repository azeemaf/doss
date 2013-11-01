package doss.core;

import java.io.IOException;
import java.nio.file.Path;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;

public abstract class WrappedBlobStore implements BlobStore {
    private final BlobStore wrapped;

    public WrappedBlobStore(BlobStore wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Blob get(long blobId) throws NoSuchBlobException, IOException {
        return wrapped.get(blobId);
    }

    @Override
    public BlobTx begin() {
        return wrapped.begin();
    }

    @Override
    public BlobTx resume(long txId) throws NoSuchBlobTxException {
        return wrapped.resume(txId);
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public Blob getLegacy(Path legacyPath) throws NoSuchBlobException,
            IOException {
        return wrapped.getLegacy(legacyPath);
    }
}
