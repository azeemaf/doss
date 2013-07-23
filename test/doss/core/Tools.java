package doss.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.skife.jdbi.v2.DBI;

import doss.Writable;
import doss.local.DirectoryContainer;
import doss.sql.SqlBlobIndex;

public class Tools {
    public static Writable stringOutput(String s) {
        final byte[] bytes = s.getBytes();
        return new Writable() {
            public void writeTo(WritableByteChannel channel) throws IOException {
                channel.write(ByteBuffer.wrap(bytes));
            }

            @Override
            public long size() throws IOException {
                return bytes.length;
            }
        };
    }

    public static void createLocalStore(Path root) throws IOException {
        Files.createDirectories(root.resolve("data"));
        Files.createDirectories(root.resolve("index/index"));
        Files.createDirectories(root.resolve("blob"));
    }
}
