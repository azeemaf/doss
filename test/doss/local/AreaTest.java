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

    Area area;
    Database db;
    Path root;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        root = folder.newFolder("fs").toPath();
        db = Database.open();
        db.migrate();
        openArea();
    }

    private void openArea() throws IOException {
        List<Filesystem> fses = new ArrayList<>();
        fses.add(new Filesystem("fs.staging", root));
        area = new Area(db, "area.staging", fses, "directory");
    }

    @After
    public void tearDown() throws Exception {
        area.close();
        db.close();
    }

    @Test
    public void testCurrentContainer() throws Exception {
        Container container = area.currentContainer();
        assertNotNull(container);
        area.setMaxContainerSize(10);
        container.put(1, Writables.wrap("hello this is a long string"));
        Container container2 = area.currentContainer();
        assertNotEquals(container.id(), container2.id());
        // ensure reopening opens the second container
        area.close();
        openArea();
        assertEquals(container2.id(), area.currentContainer().id());
    }
}
