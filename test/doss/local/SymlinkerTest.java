package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.core.Writables;

public class SymlinkerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    public DirectoryContainer container;
    public Symlinker symlinker;
    public Path root;
    public Path symlinkerPath;

    @Before
    public void setup() throws IOException {
        symlinkerPath = folder.newFolder().toPath();
        container = new DirectoryContainer(0, folder.newFolder().toPath());
        symlinker = new Symlinker(symlinkerPath);
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
        long offset = container.put(blobId, Writables.wrap(testString));
        symlinker.link(blobId, container, offset);

        Path path = symlinkerPath.resolve(Long.toString(blobId));
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
        long offset = container.put(blobId, Writables.wrap(testString));
        symlinker.link(blobId, container, offset);
        Path path = symlinkerPath.resolve(Long.toString(blobId));
        assertTrue("link path exists after remember", Files.exists(path));
        symlinker.unlink(blobId);
        assertFalse("link path does not exist after delete", Files.exists(path));

    }

}
