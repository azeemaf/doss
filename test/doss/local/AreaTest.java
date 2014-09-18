package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.core.Writables;

public class AreaTest {

    Area staging;
    Database db;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        db = Database.open();
        db.migrate();
        staging = createArea("staging");
    }

    private Area openArea(String name, Path root) throws IOException {
        List<Filesystem> fses = new ArrayList<>();
        fses.add(new Filesystem("fs." + name, root));
        return new Area(db, "area." + name, fses, "directory");
    }

    private Area createArea(String name) throws IOException {
        return openArea(name, folder.newFolder(name).toPath());
    }

    @After
    public void tearDown() throws Exception {
        staging.close();
        db.close();
    }

    @Test
    public void testCurrentContainer() throws Exception {
        Container container = staging.currentContainer();
        assertNotNull(container);
        staging.setMaxContainerSize(10);
        container.put(1, Writables.wrap("hello this is a long string"));
        Container container2 = staging.currentContainer();
        assertNotEquals(container.id(), container2.id());
        // ensure reopening opens the second container
        Path root = staging.filesystems().get(0).path();
        staging.close();
        staging = openArea("staging", root);
        assertEquals(container2.id(), staging.currentContainer().id());
    }

    @Test
    public void testReadingAnOldContainerDoesntSwitchCurrentContainer()
            throws Exception {
        Container container1 = staging.currentContainer();
        container1.put(1, Writables.wrap("hello this is a long string"));
        staging.setMaxContainerSize(10);

        Container container2 = staging.currentContainer();
        assertEquals(container2.id(), staging.currentContainer().id());
        staging.container(container1.id());
        assertEquals(container2.id(), staging.currentContainer().id());
    }

    @Test
    public void testMovingContainer()
            throws Exception {
        try (Area master = createArea("master")) {
            Container container1 = staging.currentContainer();
            container1.put(1, Writables.wrap("hello this is a long string"));
            master.moveContainerFrom(staging, container1.id());
        }
    }
}
