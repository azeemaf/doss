package doss.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import doss.SizedWritable;
import doss.Writable;

/**
 * Utility methods for constructing Writables from various objects.
 */
public class Writables {

    public static Writable wrap(final Path path) {
        return new SizedWritable() {
            @Override
            public void writeTo(WritableByteChannel targetChannel)
                    throws IOException {
                try (FileChannel sourceChannel = (FileChannel) Files
                        .newByteChannel(path, StandardOpenOption.READ)) {
                    sourceChannel.transferTo(0, Long.MAX_VALUE, targetChannel);
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
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
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

}
