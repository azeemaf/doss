package doss.local;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Iterator;

import doss.Blob;
import doss.Writable;

/**
 * A very simple container that just stores blobs as files in a directory.
 */
class DirectoryContainer implements Container {

    final long id;
    final Path dir;
    final FileChannel lockChannel;

    public DirectoryContainer(long id, Path dir) throws IOException {
        this.id = id;
        this.dir = dir;
        try {
            Files.createDirectory(dir);
        } catch (FileAlreadyExistsException e) {
            // okay
        }
        lockChannel = FileChannel.open(dir.resolve(".lock"), CREATE, WRITE);
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
            try (FileLock lock = lock();
                    WritableByteChannel channel = Files.newByteChannel(
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
    public void close() throws IOException {
        lockChannel.close();
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

    private class DirIterator implements Iterator<Blob> {
        Iterator<File> fileIter = Arrays.asList(
                dir.toFile().listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File arg0, String name) {
                        return !name.contains(".");
                    }
                })).iterator();

        @Override
        public boolean hasNext() {
            return fileIter.hasNext();
        }

        @Override
        public Blob next() {
            File f = fileIter.next();
            try {
                return get(Long.parseLong(f.getName()));
            } catch (NumberFormatException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public Iterator<Blob> iterator() {
        return new DirIterator();
    }

    @Override
    public void permanentlyDelete() throws IOException {
        for (File f : dir.toFile().listFiles()) {
            f.delete();
        }
        Files.delete(dir);
    }

    @Override
    public FileLock lock() throws IOException {
        return lockChannel.lock();
    }

}
