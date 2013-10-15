package doss.local;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import doss.Blob;
import doss.SizedWritable;
import doss.Writable;
import doss.core.Container;
import doss.core.Writables;

public class TarContainer implements Container {

    final private Path path;
    final private long id;
    final private SeekableByteChannel channel;
    final private ByteBuffer headerBuffer = ByteBuffer
            .allocate(TAR_ENTRY_HEADER_LENGTH);
    private static final int TAR_ENTRY_HEADER_LENGTH = 512;
    private static final byte[] FOOTER_BYTES = new byte[2 * TAR_ENTRY_HEADER_LENGTH];

    TarContainer(long id, Path path) throws IOException, ArchiveException {
        this.path = path;
        this.id = id;
        channel = Files.newByteChannel(path, READ, WRITE, CREATE);
    }

    @Override
    public synchronized void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized Blob get(long offset) throws IOException {
        TarBlob tarBlob = null;

        channel.position(offset);
        headerBuffer.clear();
        channel.read(headerBuffer);
        headerBuffer.flip();
        TarArchiveEntry entry = new TarArchiveEntry(headerBuffer.array());
        tarBlob = new TarBlob(path, offset + 512, entry);

        return tarBlob;
    }

    @Override
    public synchronized long put(long id, Writable data) throws IOException {
        SizedWritable output = Writables.toSized(data);
        if (channel.size() > 1024) {
            channel.position(channel.size() - 1024);
        }

        long offset = channel.position();
        writeRecordHeader(id, output);
        output.writeTo(channel);
        writeRecordPadding();
        writeArchiveFooter();
        return offset;

    }

    private void writeRecordPadding() throws IOException {
        int padding = (int) (512L - channel.position() % 512L);
        channel.write(ByteBuffer.allocate(padding));
    }

    /**
     * Write the 1024 zero byte end of archive marker.
     */
    private void writeArchiveFooter() throws IOException {
        ByteBuffer endByteByffer = ByteBuffer.wrap(FOOTER_BYTES);
        endByteByffer.flip();
        endByteByffer.rewind();
        channel.write(endByteByffer);
    }

    private void writeRecordHeader(long id, SizedWritable output)
            throws IOException {
        TarArchiveEntry entry = new TarArchiveEntry(String.valueOf(id));
        entry.setSize(output.size());
        headerBuffer.clear();
        entry.writeEntryHeader(headerBuffer.array());
        channel.write(headerBuffer);
    }

    @Override
    public long id() {
        return id;
    }

}
