package doss.local;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import doss.Writable;

/**
 * A very simple container that just stores blobs as files in a directory.
 */
class DirectoryContainer implements Container {

    final long id;
    final Path dir;

    public DirectoryContainer(long id, Path dir) throws IOException {
        this.id = id;
        this.dir = dir;
        try {
            Files.createDirectory(dir);
        } catch (FileAlreadyExistsException e) {
            // okay
        }
    }

    @Override
    public FileBlob get(long offset) throws IOException {
        String id = new String(Files.readAllBytes(idPathFor(offset)), "UTF-8");
        return new FileBlob(Long.parseLong(id), dataPathFor(offset));
    }

    @Override
    public long put(long id, Writable output) throws IOException {
        Long offset = 0L;
        while (true) {
            try (WritableByteChannel channel = Files.newByteChannel(
                    dataPathFor(offset), CREATE_NEW, WRITE)) {
                output.writeTo(channel);
                Files.write(idPathFor(offset),
                        Long.toString(id).getBytes("UTF-8"), CREATE_NEW, WRITE);
                return offset;
            } catch (FileAlreadyExistsException e) {
                offset++;
            }
        }
    }

    Path dataPathFor(long offset) {
        return dir.resolve(Long.toString(offset));
    }

    protected Path idPathFor(long offset) {
        return dir.resolve(Long.toString(offset) + ".id");
    }

    @Override
    public void close() {
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public long size() throws IOException {
        // TODO optimize
        SizeCalculator counter = new SizeCalculator();
        Files.walkFileTree(dir, counter);
        return counter.size;
    }

    private static class SizeCalculator extends SimpleFileVisitor<Path> {
        long size = 0;

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
            size += Files.size(file);
            return CONTINUE;
        }
    }

}
