package doss.net;

import java.io.IOException;
import java.net.Socket;

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
            return new RemoteBlobTx(client, client.beginTx());
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
}
