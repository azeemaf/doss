package doss.net;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;
import doss.Writable;
import doss.core.Writables;

public class RemoteBlobStore implements BlobStore {
    private final DossService.Client client;

    public RemoteBlobStore(Socket socket) throws IOException,
            TTransportException {
        TTransport transport = new TSocket(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new DossService.Client(protocol);
    }

    @Override
    public synchronized Blob get(long blobId) throws NoSuchBlobException,
            IOException {
        try {
            return new RemoteBlob(client, client.stat(blobId));
        } catch (RemoteNoSuchBlobException e) {
            throw new NoSuchBlobException(e.getBlobId());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BlobTx begin() {
        try {
            return new RemoteBlobTx(client.beginTx());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BlobTx resume(long txId) throws NoSuchBlobTxException {
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    private class RemoteBlobTx implements BlobTx {
        private final long id;

        RemoteBlobTx(long id) {
            this.id = id;
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public Blob put(Writable output) throws IOException {
            try {
                final long handle = client.beginPut(id);
                output.writeTo(new WritableByteChannel() {

                    @Override
                    public boolean isOpen() {
                        return true;
                    }

                    @Override
                    public void close() throws IOException {
                    }

                    @Override
                    public int write(ByteBuffer b) throws IOException {
                        try {
                            int start = b.position();
                            client.write(handle, b);
                            return b.position() - start;
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                return get(client.finishPut(handle));
            } catch (NoSuchBlobException | TException e) {
                throw new RuntimeException(e);
            }
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

}
