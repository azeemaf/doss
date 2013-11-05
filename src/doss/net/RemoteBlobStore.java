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
import doss.core.ManagedTransaction;
import doss.core.Transaction;
import doss.core.Writables;

public class RemoteBlobStore implements BlobStore {
    private final DossService.Client client;
    private final TTransport transport;

    public RemoteBlobStore(Socket socket) throws IOException,
            TTransportException {
        transport = new TSocket(socket);
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
    public Blob getLegacy(Path legacyPath) throws NoSuchBlobException,
            IOException {
        try {
            return new RemoteBlob(client, client.statLegacy(legacyPath
                    .toString()));
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
        // TODO: check if tx exists first?
        return new RemoteBlobTx(txId);
    }

    @Override
    public void close() {
        transport.close();
    }

    public static int transfer(ByteBuffer src, ByteBuffer dest) {
        int n = Math.min(dest.remaining(), src.remaining());
        if (n > 0) {
            ByteBuffer tmp = src.duplicate();
            tmp.limit(src.position() + n);
            dest.put(tmp);
            src.position(src.position() + n);
        }
        return n;
    }

    private class RemoteBlobTx extends ManagedTransaction implements BlobTx {
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
                final long putHandle = client.beginPut(id);
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
                        int nbytes = b.remaining();
                        try {
                            client.write(putHandle, b);
                            b.position(b.limit());
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                        return nbytes;
                    }
                });
                return get(client.finishPut(putHandle));
            } catch (TException e) {
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
        protected Transaction getCallbacks() {
            return new Transaction() {

                @Override
                public void rollback() throws IOException {
                    try {
                        client.rollbackTx(id);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void prepare() throws IOException {
                    try {
                        client.prepareTx(id);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void commit() throws IOException {
                    try {
                        client.commitTx(id);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void close() throws IOException {
                }
            };
        }

    }
}
