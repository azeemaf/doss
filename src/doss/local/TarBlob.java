package doss.local;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import doss.Blob;


public class TarBlob implements Blob {
    
    
    Path containerPath;
    long offset;
    TarArchiveEntry tarEntry;
    
    
    public TarBlob(Path  containerPath, long offset, TarArchiveEntry tarEntry){
        this.containerPath = containerPath;
        this.offset = offset;
        this.tarEntry =  tarEntry;
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


    public String slurp() throws IOException {
        SeekableByteChannel sbc = this.openChannel();
         ByteBuffer buf = ByteBuffer.allocate(new Long(size()).intValue());
         sbc.read(buf);
         buf.flip();
         String encoding = System.getProperty("file.encoding");
         CharBuffer result = Charset.forName(encoding).decode(buf);
        return result.toString();
    }


    @Override
    public FileTime created() throws IOException {
        //TODO not sure how to obtain this informatiom
        return null;
    }

}

