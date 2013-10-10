package doss.net;

import java.io.Closeable;
import java.io.IOException;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import doss.BlobStore;

public class BlobStoreServer implements Runnable, Closeable {
    final private TServer server;

    public BlobStoreServer(BlobStore blobStore) throws IOException,
            TTransportException {
        DossServiceHandler handler = new DossServiceHandler(blobStore);
        DossService.Processor<DossServiceHandler> processor =
                new DossService.Processor<>(handler);
        TServerTransport serverTransport = new TServerSocket(1234);
        server = new TSimpleServer(
                new TServer.Args(serverTransport).processor(processor));
    }

    @Override
    public void run() {
        server.serve();
    }

    @Override
    public void close() {
        server.stop();
    }
}
