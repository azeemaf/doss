package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import doss.Blob;

public class TarBlob implements Blob {
    final private Path containerPath;
    final private long offset;
    final private TarArchiveEntry tarEntry;

    public TarBlob(Path containerPath, long offset, TarArchiveEntry tarEntry) {
        this.containerPath = containerPath;
        this.offset = offset;
        this.tarEntry = tarEntry;
    }

    @Override
    public long id() {
        return Long.parseLong(tarEntry.getName());
    }

    @Override
    public long size() throws IOException {
        return tarEntry.getSize();
    }

    @Override
    public InputStream openStream() throws IOException {
        return Channels.newInputStream(openChannel());
    }

    @Override
    public SeekableByteChannel openChannel() throws IOException {
        return new SubChannel(FileChannel.open(containerPath), offset, size());
    }

    @Override
    public FileTime created() throws IOException {
        return FileTime.from(tarEntry.getModTime().getTime(),
                TimeUnit.MILLISECONDS);

    }

    @Override
    public String digest(String algorithm) throws NoSuchAlgorithmException, IOException {
        try (SeekableByteChannel channel = openChannel()) {
            return Digests.calculate(algorithm, channel);
        }
    }
}
