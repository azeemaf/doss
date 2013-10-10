package doss.net;

import java.io.IOException;
import java.nio.file.Path;

import doss.Blob;
import doss.BlobTx;
import doss.Writable;
import doss.core.Writables;
import doss.net.DossService.Client;

class RemoteBlobTx implements BlobTx {
    private final long id;

    RemoteBlobTx(Client client, long id) {
        this.id = id;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public Blob put(Writable output) throws IOException {
        return null;
    }

    @Override
    public Blob put(Path source) throws IOException {
        return put(Writables.wrap(source));
    }

    @Override
    public Blob put(byte[] bytes) throws IOException {
        return put(Writables.wrap(bytes));
    }

    @Override
    public void commit() throws IllegalStateException, IOException {
    }

    @Override
    public void rollback() throws IllegalStateException, IOException {
    }

    @Override
    public void prepare() throws IllegalStateException, IOException {
    }

    @Override
    public void close() throws IllegalStateException, IOException {
    }

}
