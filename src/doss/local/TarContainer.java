package doss.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import doss.Blob;
import doss.SizedWritable;
import doss.Writable;
import doss.core.Writables;

public class TarContainer implements Container {

    final private Path path;
    final private long id;
    final private FileChannel channel;
    final private ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    private static final int BLOCK_SIZE = 512;
    private static final int HEADER_LENGTH = BLOCK_SIZE;
    private static final int FOOTER_LENGTH = 2 * BLOCK_SIZE;
    private static final byte[] FOOTER_BYTES = new byte[FOOTER_LENGTH];

    TarContainer(long id, Path path, FileChannel channel) throws IOException {
        this.path = path;
        this.id = id;
        this.channel = channel;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    private synchronized TarArchiveEntry readEntry(long offset)
            throws IOException {
        channel.position(offset);
        headerBuffer.clear();
        while (headerBuffer.hasRemaining()) {
            int nbytes = channel.read(headerBuffer);
            if (nbytes == -1) {
                return null; // end of file
            }
        }
        headerBuffer.flip();
        return new TarArchiveEntry(headerBuffer.array());
    }

    @Override
    public Blob get(long offset) throws IOException {
        return new TarBlob(path, offset + HEADER_LENGTH,
                readEntry(offset));
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

    private static long calculatePadding(long position) {
        return BLOCK_SIZE - position % BLOCK_SIZE;
    }

    private void writeRecordPadding() throws IOException {
        int padding = (int) (calculatePadding(channel.position()));
        channel.write(ByteBuffer.allocate(padding));
    }

    /**
     * Write the 1024 zero byte end of archive marker.
     */
    private void writeArchiveFooter() throws IOException {
        channel.write(ByteBuffer.wrap(FOOTER_BYTES));
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

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public Iterator<Blob> iterator() {
        return new Iterator<Blob>() {
            long pos = 0;

            @Override
            public boolean hasNext() {
                try {
                    return pos < size();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Blob next() {
                try {
                    Blob blob = get(pos);
                    pos += HEADER_LENGTH;
                    pos += blob.size();
                    pos += calculatePadding(pos);
                    return blob;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
            }
        };
    }

    @Override
    public void permanentlyDelete() throws IOException {
        Files.delete(path);
    }

    public Path path() {
        return path;
    }

    @Override
    public void fsync() throws IOException {
        channel.force(true);
    }

}
