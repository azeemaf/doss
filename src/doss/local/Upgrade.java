package doss.local;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class Upgrade {

    public static void main(String args[]) throws IOException {
        Path src = Paths.get(args[0]);
        final Path dest = Paths.get(args[1]);

        Files.walkFileTree(src, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path srcFile, BasicFileAttributes attrs)
                    throws IOException {
                if (srcFile.getFileName().toString().contains("."))
                    return CONTINUE;
                try {
                    Path idFile = Paths.get(srcFile.toString() + ".id");
                    long blobId = Long.parseLong(new String(Files.readAllBytes(idFile)));
                    Path destFile = LocalBlobStore.stagingPath(dest, blobId);
                    System.out.println(srcFile + " -> " + destFile);
                    Files.createDirectories(destFile.getParent());
                    Files.createLink(destFile, srcFile);
                } catch (NoSuchFileException e) {
                    System.out.println("err: " + srcFile + " .id missing");
                }
                return CONTINUE;
            }

        });
    }
}
