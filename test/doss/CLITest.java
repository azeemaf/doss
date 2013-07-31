package doss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class CLITest extends DOSSTest {

    @Test
    public void cliCat() throws Exception {
        Blob blob = writeTempBlob(blobStore, TEST_STRING);
        assertNotNull("Blob is not null", blob);
        assertEquals(TEST_STRING, execute("cat", blob.id()));
    }

    @Test
    public void cliGet() throws Exception {
        Blob blob = writeTempBlob(blobStore, TEST_STRING);
        execute("get", blob.id());
        Path file = Paths.get(Long.toString(blob.id()));
        String contents = new String(Files.readAllBytes(file));
        assertEquals(TEST_STRING, contents);
        Files.deleteIfExists(file);

    }

    @Test
    public void cliPut() throws Exception {
        Path tempFile = makeTempFile(TEST_STRING);

        String output = execute("put", tempFile);

        assertTrue(output.contains(tempFile.getFileName().toString()));
        assertTrue(output.contains("Created 1 blobs."));

        // first column is digits and is a blobID.
        Pattern idPattern = Pattern.compile("\n(\\d+)\\t");
        Matcher m = idPattern.matcher(output);
        String id = null;
        while (m.find()) {
            id = m.group(1);
        }
        assertNotNull(id);
        assertEquals(TEST_STRING, slurp(blobStore.get(Long.parseLong(id))));
    }

    @Test
    public void cliPutBogusFile() throws Exception {
        String output = execute("put", "this/file/should/probably/not/exist");
        assertTrue(output.contains("no such file"));
    }

    @Test
    public void cliPutDirectory() throws Exception {
        Path directoryPath = folder.newFolder("importme").toPath();
        String output = execute("put", directoryPath);
        assertTrue(output.contains("not a regular file"));
    }

    @Test
    public void cliStat() throws Exception {
        Blob blob = writeTempBlob(blobStore, TEST_STRING);
        String output = execute("stat", blob.id());
        assertTrue(output.contains(Long.toString(blob.id())));
        assertTrue(output.contains("Created"));
        assertTrue(output.contains(Integer.toString(TEST_BYTES.length)));
    }

    @Test
    public void cliVersion() throws Exception {
        String output = execute("version");
        assertTrue(output.toString().contains("DOSS 2"));
        assertTrue(output.toString().contains("Java version:"));
        assertTrue(output.toString().contains("Java home:"));
    }

    private String execute(Object... args) throws IOException {
        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outputStream);

        String[] stringArgs = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            stringArgs[i] = args[i].toString();
        }

        try {
            System.setOut(out);
            System.setErr(out);
            System.setProperty("doss.home", blobStoreRoot.toString());
            Main.main(stringArgs);
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
        }

        return outputStream.toString("UTF-8");
    }

}
