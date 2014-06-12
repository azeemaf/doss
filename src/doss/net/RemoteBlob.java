package doss.net;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;

import org.apache.thrift.TException;

import doss.Blob;

class RemoteBlob implements Blob {
    private final DossService.Client client;
    private final StatResponse stat;

    RemoteBlob(DossService.Client client, StatResponse stat) {
        this.client = client;
        this.stat = stat;
    }

    @Override
    public long size() throws IOException {
        return stat.getSize();
    }

    @Override
    public long id() {
        return stat.getBlobId();
    }

    @Override
    public InputStream openStream() throws IOException {
        return Channels.newInputStream(openChannel());
    }

    @Override
    public SeekableByteChannel openChannel() throws IOException {
        return new RemoteChannel(client, stat);
    }

    @Override
    public FileTime created() throws IOException {
        if (stat.isSetCreatedMillis()) {
            return FileTime.fromMillis(stat.getCreatedMillis());
        } else {
            return null;
        }
    }

    @Override
    public String digest(String algorithm) throws NoSuchAlgorithmException, IOException {
        try {
            return client.digest(id(), algorithm);
        } catch (TException e) {
            throw new IOException(e);
        }
    }
}