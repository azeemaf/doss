package doss.net;

import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class OpenBench {
    static int readSize = 8192;
    static ByteBuffer b = ByteBuffer.allocate(readSize);
    static Path testFile;

    public static void main(String args[]) throws IOException {
        testFile = Paths.get(args[0]);

        for (int i = 0; i < 10; i++) {
            testSequential();
            testSequentialAndSeek();
            testReopenAndSeek();
        }
    }

    private static void testSequential() throws IOException {
        long start = System.currentTimeMillis();
        try (SeekableByteChannel c = Files.newByteChannel(testFile, READ)) {
            while (true) {
                b.clear();
                int nbytes = c.read(b);
                if (nbytes <= 0)
                    break;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("testSequential " + (end - start) + "ms");
    }

    private static void testSequentialAndSeek() throws IOException {
        long position = 0;
        long start = System.currentTimeMillis();
        try (SeekableByteChannel c = Files.newByteChannel(testFile, READ)) {
            while (true) {
                b.clear();
                c.position(position);
                int nbytes = c.read(b);
                if (nbytes <= 0)
                    break;
                position += nbytes;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("testSequentialAndSeek " + (end - start) + "ms");
    }

    private static void testReopenAndSeek() throws IOException {
        long position = 0;
        long start = System.currentTimeMillis();
        while (true) {
            try (SeekableByteChannel c = Files.newByteChannel(testFile, READ)) {
                b.clear();
                c.position(position);
                int nbytes = c.read(b);
                if (nbytes <= 0)
                    break;
                position += nbytes;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("testReopenAndSeek " + (end - start) + "ms");
    }

}
