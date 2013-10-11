package doss.local;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import doss.Blob;
import doss.SizedWritable;
import doss.Writable;
import doss.core.Container;
import doss.core.Writables;

public class TarContainer implements Container {

    Path path;
    final private long id;
    SeekableByteChannel containerChannel;
    ArchiveOutputStream tarOut;
    private static final int TAR_ENTRY_HEADER_LENGTH = 512;

    TarContainer(long id, Path path) throws IOException, ArchiveException {

        this.path = path;
        this.id = id;

        if (!path.toFile().exists()) {
            path.toFile().createNewFile();
        }
        containerChannel = Files.newByteChannel(path,
                EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE));
    }

    @Override
    public void close() {

        try {
            if (tarOut != null) {
                tarOut.finish();
            }
            containerChannel.close();
        } catch (IOException e) {
            throw new TarContainerException("Unable to close tar container "
                    + path, e.getCause());
        }
    }

    @Override
    public Blob get(long offset) throws IOException {

        if (offset < TAR_ENTRY_HEADER_LENGTH) {
            throw new IllegalArgumentException(
                    "Tar offset can not be less than 512");
        }
        TarBlob tarBlob = null;

        SubChannel subChannel = new SubChannel(containerChannel, offset
                - TAR_ENTRY_HEADER_LENGTH, 512);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TAR_ENTRY_HEADER_LENGTH);
        subChannel.read(byteBuffer);
        byteBuffer.flip();
        TarArchiveEntry entry = new TarArchiveEntry(byteBuffer.array());
        tarBlob = new TarBlob(path, offset, entry);

        return tarBlob;
    }

    @Override
    public long put(long id, Writable data) throws IOException {
        SizedWritable output = Writables.toSized(data);
        Long offset = 0L;

        TarArchiveOutputStream os = null;
        tarOut = null;
        FileOutputStream outputStream = null;
        if (containerChannel.size() == 0) {
            try {
                outputStream = new FileOutputStream(path.toFile());

                tarOut = new ArchiveStreamFactory().createArchiveOutputStream(
                        ArchiveStreamFactory.TAR, outputStream);
                TarArchiveEntry entry = new TarArchiveEntry(String.valueOf(id));
                entry.setSize(output.size());
                tarOut.putArchiveEntry(entry);
                output.writeTo(Channels.newChannel(tarOut));

                offset = new Integer(TAR_ENTRY_HEADER_LENGTH).longValue();

            } catch (ArchiveException e) {
                throw new TarContainerException(
                        "Problems Creating tar archive: " + id, e.getCause());
            } finally {

                tarOut.closeArchiveEntry();
                tarOut.close();

                tarOut.flush();

            }

        } else {

            long pos = getPosition();
            TarArchiveEntry entry = new TarArchiveEntry(String.valueOf(id));
            entry.setSize(output.size());

            byte[] entryData = new byte[TAR_ENTRY_HEADER_LENGTH];

            byte[] endTarDta = new byte[2 * (TAR_ENTRY_HEADER_LENGTH)];
            ByteBuffer endByteByffer = ByteBuffer.wrap(endTarDta);

            entry.writeEntryHeader(entryData);
            ByteBuffer bb = ByteBuffer.wrap(entryData);

            containerChannel.position(pos);
            containerChannel.write(bb);

            output.writeTo(containerChannel);

            endByteByffer.flip();
            endByteByffer.rewind();
            // write the 1024 empty bytes, they are end of archive bytes
            containerChannel.write(endByteByffer);
            ((FileChannel) containerChannel).force(false);

        }

        return offset;

    }

    public long getPosition() throws IOException {
        long offset = 0L;
        long p = -512;
        byte[] emptyBlock = new byte[TAR_ENTRY_HEADER_LENGTH];
        boolean condition = true;
        byte[] bytes = new byte[TAR_ENTRY_HEADER_LENGTH];
        long position = 0L;

        while (condition) {

            p = p + TAR_ENTRY_HEADER_LENGTH;
            containerChannel.position(p);

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            offset = containerChannel.read(bb);
            bb.flip();
            if (Arrays.equals(bb.array(), emptyBlock)) {
                condition = false;
            }
            bb.clear();

        }

        position = containerChannel.position() - TAR_ENTRY_HEADER_LENGTH;

        return position;
    }

    @Override
    public long id() {

        return id;
    }

}
