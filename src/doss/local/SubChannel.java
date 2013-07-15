package doss.local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;


/**
 * SubChannel is read only so write and truncate methods are not implemented
 *
 */
public class SubChannel implements SeekableByteChannel{
    
    SeekableByteChannel containerChannel;
    long offset;
    long length;
    
    SubChannel(SeekableByteChannel containerChannel, long blobOffset, long blobLength) throws IOException {
        if(blobOffset > containerChannel.size() || blobLength >  containerChannel.size() - blobOffset){
            throw new IllegalArgumentException("Can not create SubChannel for the container channel size " + containerChannel.size() +
                    " with offset = " +  blobOffset + " and lenght = " + blobLength);
        }
        this.containerChannel = containerChannel;
        containerChannel.position(blobOffset);
        this.offset = blobOffset;
        this.length = blobLength;
       
    }

    @Override
    public boolean isOpen() {
        return containerChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        containerChannel.close();
        
    }

    @Override
    public int read(ByteBuffer b) throws IOException {
    	int originalLimit = b.limit();
    	long capacity = offset + length;
        try {
            if (b.remaining() > capacity - containerChannel.position()) {
              // buffer has more space available than we've got left in the blob
              // so temporarily change its limit so we don't read past the end of the blob
              b.limit((int)(b.position() + capacity - containerChannel.position()));
            }

            return containerChannel.read(b);
          } finally {
            // restore the original limit
            b.limit(originalLimit);
          }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
    	throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
        return containerChannel.position() - offset;
    }

    @Override
    //input parameter new position is constrained by the size and offset,
    public SeekableByteChannel position(long newPosition) throws IOException {
    	if((newPosition < offset) || (newPosition > offset + length)){
    		throw new IllegalArgumentException();
    	}
        return containerChannel.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return length;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
    	throw new NonWritableChannelException();
    }
    
    

   

}
