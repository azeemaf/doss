package doss.net;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import doss.Blob;
import doss.BlobStore;
import doss.NoSuchBlobException;
import doss.net.DossProtocol.NoSuchBlobResponse;
import doss.net.DossProtocol.Request;
import doss.net.DossProtocol.Response;
import doss.net.DossProtocol.StatRequest;
import doss.net.DossProtocol.StatResponse;

public class BlobStoreServer implements Runnable, Closeable {
    final ServerSocket serverSocket;
    final private BlobStore blobStore;
    boolean running = true;

    public BlobStoreServer(BlobStore blobStore) throws IOException {
        serverSocket = new ServerSocket(1234);
        this.blobStore = blobStore;
    }

    @Override
    public void run() {
        try {
            while (running) {
                Connection connection = new Connection(serverSocket.accept());
                connection.start();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    class Connection extends Thread {
        final Socket socket;
        final CodedOutputStream out;
        final InputStream in;

        public Connection(Socket socket) throws IOException {
            this.socket = socket;
            out = CodedOutputStream.newInstance(socket.getOutputStream());
            in = socket.getInputStream();
        }

        private void write(Message message) throws IOException {
            int typeTag = (responseType(message) << 3) | 2;
            int msgSize = message.getSerializedSize();
            out.writeRawVarint32(CodedOutputStream
                    .computeRawVarint32Size(typeTag) +
                    CodedOutputStream.computeRawVarint32Size(msgSize) +
                    msgSize);
            out.writeRawVarint32(typeTag);
            out.writeRawVarint32(message.getSerializedSize());
            message.writeTo(out);
            out.flush();
        }

        private int responseType(Message message) {
            String className = message.getClass().getSimpleName();
            for (FieldDescriptor field : Response.getDescriptor().getFields()) {
                if (field.getMessageType().getName().equals(className)) {
                    return field.getNumber();
                }
            }
            throw new IllegalArgumentException(message.getClass().getName()
                    + " not in Response union");
        }

        private Request read() throws IOException {
            return Request.parseDelimitedFrom(in);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Request request = read();
                    System.out.println("server read: " + request);
                    if (request.hasStatRequest()) {
                        handle(request.getStatRequest());
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void handle(StatRequest req) throws IOException {
            try {
                System.out.println("handling get");
                Blob blob = blobStore.get(req.getBlobId());
                write(StatResponse.newBuilder()
                        .setBlobId(req.getBlobId())
                        .setSize(blob.size())
                        .build());
            } catch (NoSuchBlobException e) {
                write(NoSuchBlobResponse.newBuilder()
                        .setBlobId(req.getBlobId()).build());
            }
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
    }
}
