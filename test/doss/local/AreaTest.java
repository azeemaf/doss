package doss.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import doss.Blob;
import doss.Writable;
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

    @Test
    @Ignore
    public void testFailedContainerMovesAreResumable() throws Exception {
        try (Area master = createArea("master")) {
            Container container1 = new WrappedContainer(
                    staging.currentContainer()) {
                boolean exploded = false;

                @Override
                public Iterator<Blob> iterator() {
                    if (!exploded) {
                        exploded = true;
                        throw new ExpectedException();
                    }
                    return super.iterator();
                }

            };
            container1.put(1, Writables.wrap("hello this is a long string"));
            try {
                master.moveContainerFrom(staging, container1.id());
                fail("expected a mock exception");
            } catch (ExpectedException e) {
                // got the exception expected, now retrying should succeed
                master.moveContainerFrom(staging, container1.id());
            }
        }
    }

    private static class ExpectedException extends RuntimeException {

    }

    private static class WrappedContainer implements Container {

        final private Container wrapped;

        public WrappedContainer(Container wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public long id() {
            return wrapped.id();
        }

        @Override
        public long size() throws IOException {
            return wrapped.size();
        }

        @Override
        public Iterator<Blob> iterator() {
            return wrapped.iterator();
        }

        @Override
        public Blob get(long offset) throws IOException {
            return wrapped.get(offset);
        }

        @Override
        public long put(long blobId, Writable output) throws IOException {
            return wrapped.put(blobId, output);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }

        @Override
        public FileLock lock() throws IOException {
            return wrapped.lock();
        }

        @Override
        public void permanentlyDelete() throws IOException {
            wrapped.permanentlyDelete();
        }

    }
}
