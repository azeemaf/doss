package doss.local;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import doss.Writable;
import doss.core.Container;
import static java.nio.file.StandardOpenOption.*;

/**
 * A very simple container that just stores blobs in a directory.
 */
public class DirectoryContainer implements Container {
    Path dir;

    DirectoryContainer(Path dir) throws IOException {
        this.dir = dir;
        try {
            Files.createDirectory(dir);
        } catch (FileAlreadyExistsException e) {
            // cool
        }
    }

    @Override
    public LocalBlob get(long offset) throws IOException {
        String id = new String(Files.readAllBytes(idPathFor(offset)), "UTF-8");
        return new LocalBlob(id, dataPathFor(offset));
    }

    @Override
    public long put(String id, Writable output) throws IOException {
        Long offset = 0L;
        while (true) {
            try (WritableByteChannel channel = Files.newByteChannel(
                    dataPathFor(offset), CREATE_NEW, WRITE)) {
                output.writeTo(channel);
                Files.write(idPathFor(offset), id.getBytes("UTF-8"),
                        CREATE_NEW, WRITE);
                return offset;
            } catch (FileAlreadyExistsException e) {
                offset++;
            }
        }
    }

    Path dataPathFor(Long offset) {
        return dir.resolve(offset.toString());
    }

    protected Path idPathFor(Long offset) {
        return dir.resolve(offset.toString() + ".id");
    }

    @Override
    public void close() {
    }

}