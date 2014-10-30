package doss.local;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.compress.archivers.ArchiveException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.DOSSTest;
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
        FileChannel channel = FileChannel.open(testTar, CREATE, READ, WRITE);
        TarContainer tarContainer = new TarContainer(1, testTar, channel);

        File file1 = createFile(testPath, "1000", "file 1 content");
        Writable newFileBytes = Writables.wrap(file1.toPath());
        long offset1 = tarContainer.put(Long.parseLong("1000"), newFileBytes);

        File file2 = createFile(testPath, "2000", "file2 content");
        newFileBytes = Writables.wrap(file2.toPath());
        long offset2 = tarContainer.put(Long.parseLong("2000"), newFileBytes);
        assertEquals(1024, offset2);

        TarBlob blob = (TarBlob) tarContainer.get(offset1);
        String content = DOSSTest.slurp(blob);
        assertEquals(content, "file 1 content");
        TarBlob blob1 = (TarBlob) tarContainer.get(offset2);
        content = DOSSTest.slurp(blob1);
        assertEquals(content, "file2 content");

        tarContainer.close();
    }

    @Test
    public void iterator() throws Exception {

        Path testTar = testPath.resolve("testget" + getTimestamp() + ".tar");
        FileChannel channel = FileChannel.open(testTar, CREATE, READ, WRITE);
        TarContainer tarContainer = new TarContainer(1, testTar, channel);

        File file1 = createFile(testPath, "1000", "file 1 content");
        Writable newFileBytes = Writables.wrap(file1.toPath());
        long offset1 = tarContainer.put(1000L, newFileBytes);

        File file2 = createFile(testPath, "2000", "file2 content");
        newFileBytes = Writables.wrap(file2.toPath());
        long offset2 = tarContainer.put(2000L, newFileBytes);
        assertEquals(1024, offset2);

        Iterator<Blob> it = tarContainer.iterator();
        assertTrue(it.hasNext());
        assertTrue(it.hasNext());
        assertEquals(DOSSTest.slurp(it.next()), "file 1 content");
        assertTrue(it.hasNext());
        assertTrue(it.hasNext());
        assertEquals(DOSSTest.slurp(it.next()), "file2 content");
        DOSSTest.slurp(it.next());
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());

        tarContainer.close();
    }

    @Test
    public void append() throws Exception {
        Path testTar = testPath.resolve("testappend" + getTimestamp() + ".tar");
        FileChannel channel = FileChannel.open(testTar, CREATE, READ, WRITE);
        TarContainer tarContainer = new TarContainer(2, testTar, channel);

        byte[] bytes = "file 4 content".getBytes("UTF-8");
        Writable newFileBytes = Writables.wrap(bytes);
        long offset = tarContainer.put(1234, newFileBytes);
        TarBlob tarBlob = (TarBlob) tarContainer.get(offset);
        String content = DOSSTest.slurp(tarBlob);
        assertEquals(bytes.length, tarBlob.size());
        assertEquals("file 4 content".length(), content.length());
        assertEquals("file 4 content", content);
        assertNotNull(tarBlob.created());

        File file2 = createFile(testPath, "5000", "file 5 content");
        newFileBytes = Writables.wrap(file2.toPath());
        long offset2 = tarContainer.put(Long.parseLong("5000"), newFileBytes);

        tarBlob = (TarBlob) tarContainer.get(offset2);
        content = DOSSTest.slurp(tarBlob);
        assertEquals("file 5 content", content);
        assertNotNull(tarBlob.created());

        File file3 = createFile(testPath, "6000", "file 6 content");
        newFileBytes = Writables.wrap(file3.toPath());
        long offset3 = tarContainer.put(Long.parseLong("6000"), newFileBytes);

        tarBlob = (TarBlob) tarContainer.get(offset3);
        content = DOSSTest.slurp(tarBlob);
        assertEquals("file 6 content", content);
        assertNotNull(tarBlob.created());

        tarContainer.close();

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
        Files.write(filePath, content.getBytes(Charset.forName("UTF-8")));
        return newFile;
    }

    @Test
    public void testContainerPath() {
        Path root = Paths.get("/area");
        TarContainerType t = new TarContainerType();
        assertEquals("/area/004/123/456/nla.doss-4123456789.tar", t.tarPath(root, 4123456789L)
                .toString());
    }
}
