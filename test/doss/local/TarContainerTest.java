package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.compress.archivers.ArchiveException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Writable;
import doss.core.Writables;

public class TarContainerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    Path testPath;

    @Before
    public void setUp() throws Exception {
        testPath = FileSystems.getDefault().getPath(
                folder.newFolder().toPath().toString(), "test");
        if (!testPath.toFile().exists()) {
            Files.createDirectory(testPath);
        }

    }

    @Test
    public void get() throws IOException, ArchiveException {

        Path testTar = testPath.resolve("testget" + getTimestamp() + ".tar");
        TarContainer tarContainer = new TarContainer(1, testTar);

        File file1 = createFile(testPath, "1000", "file 1 content");
        Writable newFileBytes = Writables.wrap(file1.toPath());
        tarContainer.put(Long.parseLong("1000"), newFileBytes);
        long position = tarContainer.getPosition();
        assertEquals(1024L, position);

        File file2 = createFile(testPath, "2000", "file2 content");
        newFileBytes = Writables.wrap(file2.toPath());
        tarContainer.put(Long.parseLong("2000"), newFileBytes);
        position = tarContainer.getPosition();
        assertEquals(2048L, position);

        TarBlob blob = (TarBlob) tarContainer.get(512);
        String content = blob.slurp();
        assertEquals(content, "file 1 content");
        TarBlob blob1 = (TarBlob) tarContainer.get(1536);
        content = blob1.slurp();
        assertEquals(content, "file2 content");

    }

    @Test
    public void append() throws Exception {
        Path testTar = testPath.resolve("testappend" + getTimestamp() + ".tar");

        TarContainer tarContainer = new TarContainer(2, testTar);

        File file1 = createFile(testPath, "4000", "file 4 content");
        Writable newFileBytes = Writables.wrap(file1.toPath());
        tarContainer.put(Long.parseLong("4000"), newFileBytes);
        assertEquals(1024, tarContainer.getPosition());
        TarBlob tarBlob = (TarBlob) tarContainer.get(512);
        String content = tarBlob.slurp();
        assertEquals("file 4 content", content);
        assertNotNull(tarBlob.created());

        File file2 = createFile(testPath, "5000", "file 5 content");
        newFileBytes = Writables.wrap(file2.toPath());
        tarContainer.put(Long.parseLong("5000"), newFileBytes);
        assertEquals(2048, tarContainer.getPosition());

        tarBlob = (TarBlob) tarContainer.get(1536);
        content = tarBlob.slurp();
        assertEquals("file 5 content", content);
        assertNotNull(tarBlob.created());

        File file3 = createFile(testPath, "6000", "file 6 content");
        newFileBytes = Writables.wrap(file3.toPath());
        tarContainer.put(Long.parseLong("6000"), newFileBytes);

        assertEquals(3072, tarContainer.getPosition());

        tarBlob = (TarBlob) tarContainer.get(2560);
        content = tarBlob.slurp();
        assertEquals("file 6 content", content);
        assertNotNull(tarBlob.created());

    }

    public String getTimestamp() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyyhmmssa");
        String formattedDate = sdf.format(date);
        return formattedDate;
    }

    private File createFile(Path parentFolder, String fileName, String content)
            throws IOException {

        Path filePath = parentFolder.resolve(fileName);
        File newFile = filePath.toFile();

        filePath.toFile().createNewFile();
        // write some content tofile
        FileWriter fw = new FileWriter(newFile.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(content);
        bw.close();
        return newFile;
    }

}
