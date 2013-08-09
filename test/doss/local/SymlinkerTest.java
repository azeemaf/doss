package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.BlobStore;
import doss.BlobTx;
import doss.core.Writables;

public class SymlinkerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    public DirectoryContainer container;
    public Symlinker symlinker;
    public Path root;
    public Path symlinkerPath;
    public Path extFolder;

    @Before
    public void setup() throws IOException {
        root = folder.newFolder().toPath();
        symlinkerPath = root;
        container = new DirectoryContainer(0, folder.newFolder().toPath());
        symlinker = new Symlinker(symlinkerPath);

        // setup for link with extension test
        extFolder = folder.newFolder("ext").toPath();
    }

    @After
    public void teardown() {
        if (container != null) {
            container.close();
            container = null;
        }
    }

    @Test
    public void testLink() throws IOException {
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
    public void testUnlink() throws IOException {
        String testString = "foo";
        long blobId = 1;
        long offset = container.put(blobId, Writables.wrap(testString));
        symlinker.link(blobId, container, offset);
        Path path = symlinkerPath.resolve(Long.toString(blobId));
        assertTrue("link path exists after remember", Files.exists(path));
        symlinker.unlink(blobId);
        assertFalse("link path does not exist after delete", Files.exists(path));

    }

    @Test
    public void testLinkWithExt() throws IOException {
        File testFile = extFolder.resolve("foo.jp2").toFile();
        testFile.createNewFile();
        FileUtils.writeStringToFile(testFile, "foo");
        LocalBlobStore.init(root);
        BlobStore store = LocalBlobStore.open(root);
        try (BlobTx tx = store.begin()) {
            Blob blob = tx.put(testFile.toPath());
            tx.commit();
            assertTrue(
                    "link file name should contain .jp2 extension",
                    Files.exists(root.resolve("blob").resolve(
                            "" + blob.id() + ".jp2")));
        }
    }
}
