package doss.local;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryStream;
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
    final Database db;

    public DirectoryContainer(Database db, long id, Path dir) throws IOException {
        this.id = id;
        this.dir = dir;
        this.db = db;
        try {
            if (!Files.exists(dir)) {
                Files.createDirectory(dir);
            }
        } catch (FileAlreadyExistsException e) {
            // okay
        } 
    }

    @Override
    public FileBlob get(long offset) throws IOException {
        String id = new String(Files.readAllBytes(idPathFor(offset)), "UTF-8");
        return new FileBlob(Long.parseLong(id), dataPathFor(offset));
    }

    private long lastOffset() throws IOException {
        Long offset = db.getContainerLastOffset(id);
        if (offset != null) {
            return offset;
        }
        try (DirectoryStream<Path> s = Files.newDirectoryStream(dir)) {
            long largest = 0L;
            for (Path p : s) {
                String filename = p.getFileName().toString();
                if (filename.endsWith(".id")) {
                    continue;
                }
                try {
                    long n = Long.parseLong(filename);
                    if (n > largest) {
                        largest = n;
                    }
                } catch (NumberFormatException e) {
                    // ignore it
                }
            }
            return largest;
        }
    }

    @Override
    public long put(long blobId, Writable output) throws IOException {
        Long offset = lastOffset() + 1;
        while (true) {
            try (WritableByteChannel channel = Files.newByteChannel(
                    dataPathFor(offset), CREATE_NEW, WRITE)) {
                long size = output.writeTo(channel);
                Files.write(idPathFor(offset), Long.toString(blobId).getBytes("UTF-8"), CREATE_NEW,
                        WRITE);
                int n = db.increaseContainerSize(id, size, offset);
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
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public long size() throws IOException {
        long size = db.getContainerSize(id);
        if (size != 0) {
            return size;
        }
        SizeCalculator counter = new SizeCalculator();
        Files.walkFileTree(dir, counter);
        db.setContainerSize(id, counter.size);
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

}
