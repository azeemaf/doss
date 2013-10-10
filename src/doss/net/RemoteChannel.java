package doss.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

import org.apache.thrift.TException;

import doss.net.DossService.Client;

class RemoteChannel implements SeekableByteChannel {
    private Client client;
    private StatResponse stat;
    private long position = 0;

    public RemoteChannel(Client client, StatResponse stat) {
        this.client = client;
        this.stat = stat;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public SeekableByteChannel truncate(long size)
            throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public long size() throws IOException {
        return stat.getSize();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ByteBuffer bytes;
        try {
            bytes = client.read(stat.getBlobId(), position,
                    dst.remaining());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        dst.put(bytes);
        position += bytes.position();
        return bytes.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition)
            throws IOException {
        position = newPosition;
        return this;
    }

    @Override
    public long position() throws IOException {
        return position;
    }
}