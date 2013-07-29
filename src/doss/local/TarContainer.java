package doss.local;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
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
import doss.Writable;
import doss.core.Container;

public class TarContainer implements Container {

    Path path;
    final private long id;

    TarContainer( long id, Path path) throws IOException, ArchiveException {
      
        this.path = path;
        this.id = id;

        if (!path.toFile().exists()) {
            path.toFile().createNewFile();
        }

    }

    public void close() {

    }

    public Blob get(long offset) throws IOException {
        
        if (offset < 512) {
            throw new IllegalArgumentException(
                    "Tar offset can not be less than 512");
        }
        TarBlob tarBlob = null;
        try (SeekableByteChannel channel = Files.newByteChannel(path,
                EnumSet.of(StandardOpenOption.READ))) {
            
            SubChannel subChannel = new SubChannel(channel, offset - 512, 512);
            ByteBuffer byteBuffer = ByteBuffer.allocate(512);
            subChannel.read(byteBuffer);
            byteBuffer.flip();
            TarArchiveEntry entry = new TarArchiveEntry(byteBuffer.array());
            tarBlob = new TarBlob(path, offset, entry);
        }

        return tarBlob;
    }

    
    
    public long put(long id, Writable output) throws IOException {
        Long offset = 0L;

        TarArchiveOutputStream os = null;
        try (SeekableByteChannel channel = Files.newByteChannel(path,
                EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE))) {
            if (channel.size() == 0) {
                FileOutputStream outputStream = new FileOutputStream(
                        path.toFile());

                ArchiveOutputStream tarOut = null;
                try {
                    tarOut = new ArchiveStreamFactory()
                            .createArchiveOutputStream(
                                    ArchiveStreamFactory.TAR, outputStream);
                    TarArchiveEntry entry = new TarArchiveEntry(
                            String.valueOf(id));
                    entry.setSize(output.size());
                    tarOut.putArchiveEntry(entry);
                    output.writeTo(Channels.newChannel(tarOut));
                  
                    offset = 512L;

                } catch (ArchiveException e) {
                    throw new TarContainerException("Problems Creating tar archive: " + id,e.getCause());
                } finally {
                    tarOut.closeArchiveEntry();
                    tarOut.close();

                }

            } else {

                long pos = getPosition();
                TarArchiveEntry entry = new TarArchiveEntry(String.valueOf(id));
                entry.setSize(output.size());

                byte[] entryData = new byte[512];
                
                byte[] endTarDta = new byte[1024];
                ByteBuffer endByteByffer = ByteBuffer.wrap(endTarDta);

               
                entry.writeEntryHeader(entryData);
                ByteBuffer bb = ByteBuffer.wrap(entryData);

              
                channel.position(pos);
                channel.write(bb);
                output.writeTo(channel);

                endByteByffer.flip();
                endByteByffer.rewind();
                //write the 1024 empty bytes, they are end of archive bytes
                channel.write(endByteByffer);

            }

        }

        return offset;

    }

    public long getPosition() throws IOException {
        long offset = 0L;
        long p = -512;
        byte[] emptyBlock = new byte[512];
        boolean condition = true;
        byte[] bytes = new byte[512];
        long position = 0L;
        try (SeekableByteChannel channel = Files.newByteChannel(path,
                EnumSet.of(StandardOpenOption.READ))) {
            while (condition) {

                p = p + 512;
                channel.position(p);

                ByteBuffer bb = ByteBuffer.wrap(bytes);
                offset = channel.read(bb);
                bb.flip();
                if (Arrays.equals(bb.array(), emptyBlock)) {
                    condition = false;
                }
                bb.clear();

            }

            position = channel.position() - 512;
        }
        return position;
    }

 
    public long id() {

        return id;
    }

}
