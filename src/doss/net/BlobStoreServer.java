package doss.net;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import doss.BlobStore;
import doss.net.DossService.Iface;

public class BlobStoreServer implements Runnable, Closeable {
    final private TServer server;
    final Map<TTransport, Connection> handlers = new ConcurrentHashMap<>();

    public BlobStoreServer(BlobStore blobStore, ServerSocket socket)
            throws IOException,
            TTransportException {
        TServerTransport serverTransport = new TServerSocket(socket);
        server = new TSimpleServer(
                new TServer.Args(serverTransport)
                        .processorFactory(new ProcessorFactory(blobStore)));
        server.setServerEventHandler(new TServerEventHandler() {

            @Override
            public void processContext(ServerContext arg0, TTransport arg1,
                    TTransport arg2) {
            }

            @Override
            public void preServe() {
            }

            @Override
            public void deleteContext(ServerContext context, TProtocol in,
                    TProtocol out) {
                Connection conn = (Connection) context;
                handlers.remove(conn);
                conn.disconnect();
            }

            @Override
            public ServerContext createContext(TProtocol in, TProtocol out) {
                return handlers.get(in.getTransport());
            }
        });
    }

    private class ProcessorFactory extends TProcessorFactory {
        final BlobStore blobStore;

        public ProcessorFactory(BlobStore blobStore) {
            super(null);
            this.blobStore = blobStore;
        }

        @Override
        public TProcessor getProcessor(TTransport transport) {
            Connection handler = new Connection(blobStore);
            handlers.put(transport, handler);
            return new Processor(handler);
        }

        @Override
        public boolean isAsyncProcessor() {
            return false;
        }

    }

    private static class Processor extends DossService.Processor<Iface> {

        public Processor(Iface iface) {
            super(iface);
        }

        @Override
        public boolean process(TProtocol arg0, TProtocol arg1)
                throws TException {
            try {
                return super.process(arg0, arg1);
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }
        }

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
