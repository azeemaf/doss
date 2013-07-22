package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.h2.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.core.Tools;

public class SymlinkContainerIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    public DirectoryContainer container;
    public SymlinkContainerIndex index;

    @Before
    public void setup() throws IOException {
        Path containerStorage = folder.newFolder().toPath();
        Path indexStorage = folder.newFolder().toPath()
                .resolve("symlink_blob_index");
        Files.createDirectories(indexStorage);

        container = new DirectoryContainer(containerStorage);
        index = new SymlinkContainerIndex(container, indexStorage);

    }

    @After
    public void teardown() {
        if (container != null) {
            container.close();
            container = null;
        }
    }

    @Test
    public void testRemember() throws IOException {
        String testString = "foo";
        long blobId = 1;
        long offset = container.put(Long.toString(blobId),
                Tools.stringOutput(testString));
        index.remember(blobId, offset);

        Path path = index.contentPath(blobId);
        String storedString = org.apache.commons.io.IOUtils.toString(container
                .get(offset).openStream());
        String linkedString = org.apache.commons.io.IOUtils.toString(Files
                .newInputStream(path));
        assertEquals("Can read value back from container", testString,
                storedString);
        assertEquals("Can read value via index", testString, linkedString);
    }

    @Test
    public void testDelete() throws IOException {
        String testString = "foo";
        long blobId = 1;
        long offset = container.put(Long.toString(blobId),
                Tools.stringOutput(testString));
        index.remember(blobId, offset);
        Path path = index.contentPath(blobId);
        assertTrue("link path exists after remember", Files.exists(path));
        index.delete(blobId);
        assertFalse("link path does not exist after delete", Files.exists(path));

    }

}
