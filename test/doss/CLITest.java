package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import doss.core.Tools;

public class CLITest extends DOSSTest {

    @Test
    public void cliCat() throws Exception {
        Path path = folder.newFolder().toPath();
        Tools.createLocalStore(path);
        blobStore = DOSS.openLocalStore(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertNotNull("Blob is not null", blob);

        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setOut(out);
            System.setProperty("doss.home", path.toString());
            Main.main("cat", blob.id());
        } finally {
            System.setOut(oldOut);
        }

        assertEquals(TEST_STRING, outputStream.toString("UTF-8"));
    }

    @Test
    public void cliGet() throws Exception {
        Path path = folder.newFolder().toPath();
        Tools.createLocalStore(path);
        blobStore = DOSS.openLocalStore(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertNotNull("Blob is not null", blob);
        assertEquals(TEST_STRING, slurp(blob));

        System.setProperty("doss.home", path.toString());
        Main.main("get", blob.id());

        assertTrue(Arrays.equals(Files.readAllBytes(Paths.get(blob.id())),
                TEST_BYTES));
        assertEquals(TEST_STRING,
                new String(Files.readAllBytes(Paths.get(blob.id())), "UTF-8"));

        Files.deleteIfExists(Paths.get(blob.id()));

    }

    @Test
    public void cliPut() throws Exception {
        Path dossPath = folder.newFolder().toPath();
        Tools.createLocalStore(dossPath);

        Path tempFilePath = makeTempFile(TEST_STRING);

        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setOut(out);
            System.setProperty("doss.home", dossPath.toString());
            Main.main("put", tempFilePath.toString());
        } finally {
            System.setOut(oldOut);
        }

        assertTrue(outputStream.toString("UTF-8").contains(
                tempFilePath.getFileName().toString()));
        assertTrue(outputStream.toString("UTF-8").contains("Created 1 blobs."));

        // first column is digits and is a blobID.
        Pattern idPattern = Pattern.compile("\\n(\\d*)\\t");
        Matcher m = idPattern.matcher(outputStream.toString("UTF-8"));
        String id = null;
        while (m.find()) {
            id = m.group(1);
        }
        
        blobStore = DOSS.openLocalStore(dossPath);

        byte[] buf = new byte[TEST_BYTES.length];
        try (SeekableByteChannel channel = blobStore.get(id).openChannel()) {
            channel.read(ByteBuffer.wrap(buf));
        }
        assertEquals(TEST_STRING, new String(buf, "UTF-8"));
    }

    @Test
    public void cliPutBogusFile() throws Exception {
        Path dossPath = folder.newFolder().toPath();
        Tools.createLocalStore(dossPath);

        PrintStream oldErr = System.err;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setErr(out);
            System.setProperty("doss.home", dossPath.toString());
            Main.main("put", "this/file/should/probably/not/exist");
        } finally {
            System.setErr(oldErr);
        }

        assertTrue(outputStream.toString("UTF-8").contains("no such file"));
    }

    @Test
    public void cliPutDirectory() throws Exception {
        Path dossPath = folder.newFolder().toPath();
        Tools.createLocalStore(dossPath);

        PrintStream oldErr = System.err;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        Path directoryPath = folder.newFolder("importme").toPath();

        try {
            System.setErr(out);
            System.setProperty("doss.home", dossPath.toString());
            Main.main("put", directoryPath.toString());
        } finally {
            System.setErr(oldErr);
        }

        assertTrue(outputStream.toString("UTF-8")
                .contains("not a regular file"));
    }

    @Test
    public void cliStat() throws Exception {
        Path path = folder.newFolder().toPath();
        Tools.createLocalStore(path);
        blobStore = DOSS.openLocalStore(path);

        Blob blob = null;

        try (BlobTx tx = blobStore.begin()) {
            blob = tx.put(TEST_BYTES);
            tx.commit();
        }
        assertNotNull("Blob is not null", blob);

        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setOut(out);
            System.setProperty("doss.home", path.toString());
            Main.main("stat", blob.id());
        } finally {
            System.setOut(oldOut);
        }

        assertTrue(outputStream.toString().contains(blob.id()));
        assertTrue(outputStream.toString().contains("Created"));
        assertTrue(outputStream.toString().contains(
                Integer.toString(TEST_BYTES.length)));
    }

    @Test
    public void cliVersion() throws Exception {
        PrintStream oldOut = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        try {
            System.setOut(out);
            Main.main("version");
        } finally {
            System.setOut(oldOut);
        }

        assertTrue(outputStream.toString().contains("DOSS 2"));
        assertTrue(outputStream.toString().contains("Java version:"));
        assertTrue(outputStream.toString().contains("Java home:"));
    }

}
