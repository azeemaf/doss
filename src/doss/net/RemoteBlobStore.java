package doss.net;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.attribute.FileTime;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.NoSuchBlobException;
import doss.NoSuchBlobTxException;
import doss.net.DossProtocol.Request;
import doss.net.DossProtocol.Response;
import doss.net.DossProtocol.StatRequest;
import doss.net.DossProtocol.StatResponse;

public class RemoteBlobStore implements BlobStore {
    final CodedOutputStream out;
    final InputStream in;

    public RemoteBlobStore(Socket socket) throws IOException {
        out = CodedOutputStream.newInstance(socket.getOutputStream());
        in = socket.getInputStream();
    }

    @Override
    public synchronized Blob get(long blobId) throws NoSuchBlobException,
            IOException {
        write(StatRequest.newBuilder().setBlobId(blobId).build());
        Response msg = read();
        if (!msg.hasStatResponse()) {
            throw new RuntimeException("unexpected " + msg);
        }
        final StatResponse stat = msg.getStatResponse();
        return new Blob() {

            @Override
            public long size() throws IOException {
                return stat.getBlobId();
            }

            @Override
            public long id() {
                return stat.getBlobId();
            }

            @Override
            public InputStream openStream() throws IOException {
                return null;
            }

            @Override
            public SeekableByteChannel openChannel() throws IOException {
                return null;
            }

            @Override
            public FileTime created() throws IOException {
                return null;
            }
        };
    }

    @Override
    public BlobTx begin() {
        return null;
    }

    @Override
    public BlobTx resume(long txId) throws NoSuchBlobTxException {
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    private void write(Message message) throws IOException {
        // writes out a Request wrapper union
        // this is the same as
        // Request.newBuilder().setGetRequest(message).build().writeDelimitedTo(out);
        // but automatically fills in the appropriate field for any message type
        int typeTag = (requestType(message) << 3) | 2;
        int msgSize = message.getSerializedSize();
        out.writeRawVarint32(CodedOutputStream.computeRawVarint32Size(typeTag) +
                CodedOutputStream.computeRawVarint32Size(msgSize) +
                msgSize);
        out.writeRawVarint32(typeTag);
        out.writeRawVarint32(message.getSerializedSize());
        message.writeTo(out);
        out.flush();
        System.out.println("client wrote " + message);
    }

    private int requestType(Message message) {
        String className = message.getClass().getSimpleName();
        for (FieldDescriptor field : Request.getDescriptor().getFields()) {
            if (field.getMessageType().getName().equals(className)) {
                return field.getNumber();
            }
        }
        throw new IllegalArgumentException(message.getClass().getName()
                + " not in Request union");
    }

    private Response read() throws IOException {
        return Response.parseDelimitedFrom(in);
    }
}
