package doss.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import doss.Blob;
import doss.SizedWritable;
import doss.Writable;

/**
 * Utility methods for constructing Writables from various objects.
 */
public class Writables {

    public static Writable wrap(final Path path) {
        return new SizedWritable() {
            @Override
            public long writeTo(WritableByteChannel targetChannel)
                    throws IOException {
                try (FileChannel sourceChannel = (FileChannel) Files
                        .newByteChannel(path, StandardOpenOption.READ)) {
                    return copy(sourceChannel, targetChannel);
                }
            }

            @Override
            public long size() throws IOException {
                return Files.size(path);
            }
        };
    }

    public static Writable wrap(final byte[] bytes) {
        return new SizedWritable() {
            @Override
            public long writeTo(WritableByteChannel channel) throws IOException {
                return channel.write(ByteBuffer.wrap(bytes));
            }

            @Override
            public long size() {
                return bytes.length;
            }
        };
    }

    public static Writable wrap(String string) {
        return wrap(string.getBytes(Charset.forName("UTF-8")));
    }

    public static SizedWritable toSized(Writable writable) {
        if (writable instanceof SizedWritable) {
            return (SizedWritable) writable;
        }
        throw new IllegalArgumentException("toSized not yet impemented for "
                + writable.getClass());
    }

    private static long copy(ReadableByteChannel in, WritableByteChannel out)
            throws IOException {
        if (in instanceof FileChannel) {
            FileChannel infc = (FileChannel) in;
            long size = infc.size();
            long copied = 0;
            while (copied < size) {
                long nread = infc.transferTo(copied, Long.MAX_VALUE, out);
                if (nread > 0) {
                    copied += nread;
                }
            }
            return copied;
        } else {
            long copied = 0;
            ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024);
            while (in.read(buffer) != -1) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    long written = out.write(buffer);
                    if (written > 0) {
                        copied += written;
                    }
                }
                buffer.compact();
            }
            return copied;
        }
    }

    public static Writable wrap(final Blob blob) {
        return new SizedWritable() {
            @Override
            public long writeTo(WritableByteChannel out) throws IOException {
                try (SeekableByteChannel in = blob.openChannel()) {
                    return copy(in, out);
                }
            }

            @Override
            public long size() throws IOException {
                return blob.size();
            }
        };
    }

}
