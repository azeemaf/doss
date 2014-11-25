package doss.net;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.thrift.transport.TTransportException;

import doss.BlobStore;
import doss.core.WrappedBlobStore;
import doss.local.TempBlobStore;

public class LoopbackBlobStore extends WrappedBlobStore {

    private final BlobStoreServer server;

    private LoopbackBlobStore(BlobStoreServer server, RemoteBlobStore client) {
        super(client);
        this.server = server;
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            server.close();
        }
    }

    /**
     * Creates a LoopbackBlobStore wrapping a TempBlobStore. For testing only.
     */
    public static BlobStore open() throws IOException, TTransportException {
        return open(TempBlobStore.open());
    }

    public static BlobStore open(BlobStore wrapped) throws IOException,
            TTransportException {
        BlobStoreServer server = new BlobStoreServer(wrapped, 0, -1,
                InetAddress.getLoopbackAddress());
        Thread serverThread = new Thread(server);
        serverThread.start();
        RemoteBlobStore client = RemoteBlobStore.openSecure(server.getInetAddress()
                .getHostAddress(), server.getPort());
        return new LoopbackBlobStore(server, client);
    }
}
